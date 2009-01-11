package net.obsearch.index.knngraph.impl;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Transaction;
import org.neo4j.api.core.TraversalPosition;

import cern.colt.bitvector.BitVector;
import cern.colt.bitvector.QuickBitVector;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.filter.Filter;
import net.obsearch.index.IndexShort;
import net.obsearch.index.bucket.impl.BucketContainerShort;
import net.obsearch.index.bucket.impl.BucketObjectShort;
import net.obsearch.index.knngraph.AbstractKnnGraph;
import net.obsearch.ob.OBShort;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueInvertedShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultInvertedShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;

public class KnnGraphShort<O extends OBShort>
		extends
		AbstractKnnGraph<O, BucketObjectShort<O>, OBQueryShort<O>, BucketContainerShort<O>>
		implements IndexShort<O> {

	/**
	 * Pivots used when the z-order is not working properly.
	 * 
	 */
	private long[][] iDistanceSeeds;

	private OBPriorityQueueInvertedShort<Long> furthest;

	public KnnGraphShort(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount,
			int localk, short maxResult, int maxSeeds)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount, localk);
		int i = 0;
		iDistanceSeeds = new long[pivotCount][];
		while (i < pivotCount) {
			iDistanceSeeds[i] = new long[maxResult];
			int cx = 0;
			while (cx < iDistanceSeeds[i].length) {
				iDistanceSeeds[i][cx] = -1;
				cx++;
			}
			i++;
		}
		furthest = new OBPriorityQueueInvertedShort<Long>(maxSeeds);
	}

	/**
	 * TODO: test if the element exists in the DB. Put this method as
	 * insertBucketBulk
	 */
	protected OperationStatus insertBucketBulk(BucketObjectShort b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		byte[] code = getAddress(b);

		Transaction tx = neo.beginTx();

		// we have to create a node
		try {

			ByteBuffer pointer = Buckets.getValue(code);
			Node n = null;
			if (pointer == null) {
				n = neo.createNode();
				ByteBuffer id = ByteConversion.longToByteBuffer(n.getId());
				Buckets.put(code, id);
			} else {
				// get the node from the DB.
				long id = ByteConversion.byteBufferToLong(pointer);
				n = neo.getNodeById(id);
			}
			updateSeeds(b, object, n.getId());
			fillNode(n, b);			
			// now we have to search all the nodes and calculate the
			// distance b
			CloseIterator<TupleBytes> it = Buckets.processAll();
			// get the lowest distance of the bucket and the rest of the
			// items
			long avg = 0;
			int count = 0;
			while (it.hasNext()) {
				TupleBytes other = it.next();
				long otherId = ByteConversion
						.byteBufferToLong(other.getValue());
				
				Node otherN = neo.getNodeById(otherId);
				// update the k nearest neighbors of this node
				// and my node
				short dist = updateRelations(n, b, otherN);
				tx.success();
				/*if (dist < lowest) {
					lowest = dist;
				}*/
				avg += dist;
				count ++;
							
			}
			short av = (short)(avg/count);
			it.closeCursor();
			if (furthest.isCandidate(av)) {
				this.furthest.addMax(n.getId(), n.getId(), av);
			}
			//tx.success();	
		} finally {
			tx.finish();

		}

		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
		res.setId(b.getId());
		return res;
	}

	@Override
	protected void fillNodeAux(Node n, BucketObjectShort bucket)
			throws OBException {
		// add the smap tuple if there is none
		if (n.hasProperty(PROP_SMAP)) {
			short[] tuple = (short[]) n.getProperty(PROP_SMAP);
			OBAsserts.chkAssert(Arrays.equals(tuple, bucket.getSmapVector()),
					"Smap vectors do not match!");
		} else {
			// put the smap vector.
			n.setProperty(PROP_SMAP, bucket.getSmapVector());
		}

	}

	/**
	 * Convert value into a binary string with 0's padded to the left.
	 * 
	 * @param value
	 * @return
	 */
	private char[] convert(short value) {
		StringBuilder res = new StringBuilder();
		String base = Long.toBinaryString(value);
		int i = 0;
		final int max = ByteConstants.Short.getBits() - base.length();

		while (i < max) {
			res.append("0");
			i++;
		}
		res.append(base);
		char[] result = res.toString().toCharArray();
		assert result.length == ByteConstants.Short.getBits();
		return result;
	}

	private char[] convert(short[] values) {
		char[] res = new char[values.length * ByteConstants.Short.getBits()];
		int i = 0; // values
		int cx = 0; // result
		while (i < values.length) {
			char[] t = convert(values[i]);
			System.arraycopy(t, 0, res, cx, t.length);
			cx += t.length;
			i++;
		}
		return res;
	}

	protected byte[] zOrder(short[] t) {

		// get the binary representation of all the
		char[] input = convert(t);
		char[] output = new char[input.length];
		// get all the bits and put them into the bit vector.
		int i = 0; // destination bits
		int ax = 0; // position in the bit set (inside each short).
		while (ax < ByteConstants.Short.getBits()) {
			int cx = 0;
			while (cx < t.length) { // for each short, take the ith bit
				char bit = input[cx * ByteConstants.Short.getBits() + ax];
				output[i] = bit;
				i++;
				cx++;
			}
			ax++;
		}

		int cx = output.length - 1;
		BigInteger result = BigInteger.ZERO;
		i = 0;
		while (cx >= 0) {
			if (input[cx] == '1') {
				result = result.setBit(i);
			}
			cx--;
			i++;
		}
		return fact.serializeBigInteger(result);
	}

	@Override
	public byte[] getAddress(BucketObjectShort bucket) {
		short[] t = bucket.getSmapVector();
		// return Buckets.prepareBytes( grayCode(t));
		return zOrder(t);
	}

	/**
	 * Update the relationships of otherN based on my node n, and bucket b.
	 * 
	 * @param n
	 *            Newcomer node
	 * @param b
	 *            Bucket of the node
	 * @param otherN
	 *            The other node from the DB.
	 * @return distance or Short.MAX_VALUE if the node was not processed
	 */
	protected short updateRelations(Node n, BucketObjectShort b, Node otherN) {

		if (n.getId() != otherN.getId()) {
			short[] nSmap = (short[]) n.getProperty(PROP_SMAP);
			short[] otherNSmap = (short[]) otherN.getProperty(PROP_SMAP);
			short linfResult = BucketObjectShort.lInf(nSmap, otherNSmap);
			updateRelationsAux(linfResult, n, otherN);
			updateRelationsAux(linfResult, otherN, n);
			return linfResult;
		}
		return Short.MAX_VALUE;
	}

	/**
	 * Update node b from a based on the linf value obtained.
	 * 
	 * @param linf
	 * @param a
	 *            Target node
	 * @param b
	 *            Node that will be modified.
	 */
	private void updateRelationsAux(short linf, Node a, Node b) {
		//Transaction tx = neo.beginTx();
		try {
			Iterable<Relationship> it = b.getRelationships(RelTypes.NN,
					Direction.OUTGOING);
			int i = 0; // count how many it has.
			Relationship largest = null; // take the largest rel and replace it
											// with
			// our new relation.
			short largestValue = Short.MIN_VALUE;
			for (Relationship r : it) {
				short value = (Short) r.getProperty(PROP_VAL);
				if (value > largestValue) {
					largest = r;
					largestValue = value;
				}
				i++;
			}
			if (i < super.localk) {
				// we can add the object because k is not yet full
				Relationship r = b.createRelationshipTo(a, RelTypes.NN);
				r.setProperty(PROP_VAL, Short.valueOf(linf));
				// otherwise, delete the largest item
			} else if (largestValue > linf) {
				largest.delete();
				Relationship r = b.createRelationshipTo(a, RelTypes.NN);
				r.setProperty(PROP_VAL, Short.valueOf(linf));
			}
			//tx.success();
		} finally {
			//tx.finish();
		}

	}

	@Override
	public BucketObjectShort getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		short[] smap = BucketObjectShort.convertTuple(object, super.pivots);
		return new BucketObjectShort(smap, -1);

	}

	@Override
	protected BucketContainerShort<O> instantiateBucketContainer(
			ByteBuffer data, byte[] address) {
		return null;
	}

	@Override
	protected int primitiveDataTypeSize() {
		return ByteConstants.Short.getSize();
	}

	@Override
	public Iterator<Long> intersectingBoxes(O object, short r)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean intersects(O object, short r, int box)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		// TODO Auto-generated method stub
		return false;
	}
	
	

	/**
	 * 
	 * Find seeds based on the z-order
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * 
	 * @param q
	 *            query
	 * @param b
	 *            bucket
	 * @param visited
	 *            visited list
	 * @param searchQueue
	 *            the queue used to initialize seeds
	 * @param localk
	 *            number of seeds to find.
	 * @throws OBStorageException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws OBException
	 */
	private void findZSeeds(OBQueryShort<O> q, BucketObjectShort b,
			HashSet<Node> visited,
			OBPriorityQueueInvertedShort<Node> searchQueue, int seeds)
			throws InstantiationException, IllegalAccessException, OBException {
		// byte[] min = Buckets.prepareBytes(grayCode(q.getLow()));
		// byte[] max = Buckets.prepareBytes(grayCode(q.getHigh()));
		// byte[] center = Buckets.prepareBytes(grayCode(b.getSmapVector()));
		byte[] min = zOrder(q.getLow());
		byte[] max = zOrder(q.getHigh());
		byte[] center = zOrder(b.getSmapVector());
		// all the values returned by the iterators will be within
		// max and min.
		CloseIterator<TupleBytes> it = Buckets.processRange(center, max);
		CloseIterator<TupleBytes> it3 = Buckets
				.processRangeReverse(center, max);
		// CloseIterator<TupleBytes> it3 = Buckets.processRangeReverse(center,
		// max);
		CloseIterator<TupleBytes> it2 = Buckets
				.processRangeReverse(min, center);
		CloseIterator<TupleBytes> it4 = Buckets.processRange(min, center);
		if (it2.hasNext()) {
			it2.next(); // skip the center
		}
		// find a couple of nodes
		int i = 0;
		int which = 1; // if it or it2 will be read.
		int insertedSeeds = 0; // count the average # of seeds

		while ((it.hasNext() || it2.hasNext() || it3.hasNext() || it4.hasNext())
				&& searchQueue.getSize() < seeds) {
			TupleBytes t = null;
			if (which == 1 && it.hasNext()) {
				t = it.next();
			}
			if (which == 4 && it2.hasNext()) {
				t = it2.next();
			}
			if (which == 3 && it3.hasNext()) {
				t = it3.next();
			}
			if (which == 2 && it4.hasNext()) {
				t = it4.next();
			}
			if (t != null) {
				// process from this node.
				long id = ByteConversion.byteBufferToLong(t.getValue());
				Node seed = neo.getNodeById(id);
				// all the distances are with respect to the query.
				short distance = lInf(b.getSmapVector(), seed);
				if (visited.add(seed)) {
					searchQueue.add(id, seed, distance);
					insertedSeeds++;
				}

			}
			// change which
			if (which == 4) {
				which = 1;
			} else {
				which++;
			}
			i++;
		}
		stats.addExtraStats("seedsZ", insertedSeeds);
		it.closeCursor();
		it2.closeCursor();
		it3.closeCursor();
		it4.closeCursor();
		// seeds were not found...
		// range queries is ok, but in knn things can get wrong.
	}

	/**
	 * Find seeds based on the objects that are far far away from all the other
	 * objects
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * 
	 */

	private void findFarSeeds(OBQueryShort<O> q, BucketObjectShort b,
			HashSet<Node> visited,
			OBPriorityQueueInvertedShort<Node> searchQueue, int seeds)
			throws InstantiationException, IllegalAccessException {
		List<OBResultInvertedShort<Long>> r = this.furthest.getSortedElements();
		for (OBResultInvertedShort<Long> n : r) {
			if (searchQueue.getSize() == seeds) {
				break;
			}
			Node node = neo.getNodeById(n.getObject());
			short distance = lInf(b.getSmapVector(), node );
			searchQueue.add(n.getId(), node, distance);
		}
	}

	/**
	 * Find seeds using the iDistance pivots selected.
	 * 
	 * @param q
	 * @param b
	 * @param visited
	 * @param searchQueue
	 * @param seeds
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws OBException
	 */
	private void findISeeds(OBQueryShort<O> q, BucketObjectShort b,
			HashSet<Node> visited,
			OBPriorityQueueInvertedShort<Node> searchQueue, int seeds)
			throws InstantiationException, IllegalAccessException, OBException {

		short[] centerLeft = new short[getPivotCount()];
		short[] centerRight = new short[getPivotCount()];
		int i = 0;
		while (i < getPivotCount()) {
			centerLeft[i] = (short) Math.max(0, centerLeft[i] - 1);
			i++;
		}

		System.arraycopy(b.getSmapVector(), 0, centerLeft, 0, getPivotCount());
		System.arraycopy(b.getSmapVector(), 0, centerRight, 0, getPivotCount());
		boolean continueLeft = true;
		boolean continueRight = true;
		while ((continueLeft || continueRight) && searchQueue.getSize() < seeds) {

			if (continueRight) {
				continueRight = findISeedsAux(centerRight, q, b, visited,
						searchQueue, seeds, 1);
			}

			if (continueLeft) {
				continueLeft = findISeedsAux(centerLeft, q, b, visited,
						searchQueue, seeds, -1);
			}

		}

	}

	private boolean findISeedsAux(short[] vect, OBQueryShort<O> q,
			BucketObjectShort b, HashSet<Node> visited,
			OBPriorityQueueInvertedShort<Node> searchQueue, int seeds, int inc)
			throws InstantiationException, IllegalAccessException {
		int dim = 0; // dimension index
		// match right.
		int proc = 0;
		while (dim < getPivotCount() && searchQueue.getSize() < seeds) {
			// find an object in the right size (+)
			boolean cont = true;
			while (cont && vect[dim] < this.iDistanceSeeds[dim].length
					&& vect[dim] >= 0 && searchQueue.getSize() < seeds) {
				if (this.iDistanceSeeds[dim][vect[dim]] != -1) {
					long id = this.iDistanceSeeds[dim][vect[dim]];
					Node seed = neo.getNodeById(id);
					// all the distances are with respect to the query.
					short distance = lInf(b.getSmapVector(), seed);
					if (visited.add(seed)) {
						searchQueue.add(id, seed, distance);
						proc++;
						cont = false;
					}

				}

				vect[dim] = (short) (vect[dim] + inc);

			}
			dim++;
		}
		return proc != 0; // return that we did modify something.
	}

	// change the pivots to idistance, cheaper and easier to understand
	// I will know that it works.
	@Override
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		// get the gray code.
		BucketObjectShort b = getBucket(object);
		OBQueryShort<O> q = new OBQueryShort<O>(object, r, result, b
				.getSmapVector());

		OBPriorityQueueInvertedShort<Node> searchQueue = new OBPriorityQueueInvertedShort<Node>(
				-1);
		HashSet<Node> visited = new HashSet<Node>();

		//findZSeeds(q, b, visited, searchQueue, (int) (3));

		//findISeeds(q, b, visited, searchQueue, (int) (seeds * .20 ) );

		findFarSeeds(q, b, visited, searchQueue, seeds);

		assert searchQueue.getSize() == this.seeds;
		/*OBResultInvertedShort<Node> n = searchQueue.poll();
		while(n != null){
			OBPriorityQueueInvertedShort<Node> searchQueue2 = new OBPriorityQueueInvertedShort<Node>(
					-1);
			searchQueue2.add(n.getId(), n.getObject(), n.getDistance());
			searchAux(object, searchQueue2, b, q, visited);
			n = searchQueue.poll();
		}*/
		
		searchAux(object, searchQueue, b, q, visited);
		
	}
	
	private void searchAux(O object, OBPriorityQueueInvertedShort<Node> searchQueue, BucketObjectShort b, OBQueryShort<O> q , HashSet<Node> visited) throws IllegalIdException, IllegalAccessException, InstantiationException, OBException{
		// search each node, hope for the best.

		
		// we have the seeds
		// 1) take the top of the queue
		// 2) find the neighbors that are t-closer with respect to the query
		// 3) search stops when the element at the front is not t-closer
		// to the current nearest neighbor with respect to the query
		// this means ! d(front, curr) <= t * d(curr, q)
		Transaction txn = neo.beginTx();
		try {
			OBResultInvertedShort<Node> curr = null;
			while (searchQueue.getSize() > 0) {
				// take the top of the queue
				OBResultInvertedShort<Node> front = searchQueue.poll();
				

				// d(front, q)
				short frontAndQ = front.getDistance();
				// if the object is within range, we can add it to the result.
				if (q.isCandidate(frontAndQ)) {
					// get all the objects and match them.
					for (long id : (long[]) front.getObject().getProperty(
							PROP_IDS)) {
						O o = this.getObject(id);
						short distance = object.distance(o);
						stats.incDistanceCount();

						if (distance <= q.getDistance()) {
							q.add(id, o, distance);
						}
					}
				}

				// check if we should stop if the front is not t-closer
				// to the current nearest with respect to the query
				// that is, ! d(front, curr) <= t d(q,curr)
				if (curr != null) {
					// d(curr, q)
					short currAndQ = curr.getDistance();
					short frontAndCurr = lInf(curr.getObject(), front
							.getObject());
					// if (!tCloser(frontAndCurr, currAndQ)) {
					if (!tCloser(frontAndCurr, currAndQ)) {
						break; // finish search
					}
					if (curr.getDistance() > frontAndQ) {
						curr = front;
					}
				} else {
					curr = front;
				}

				int i = 0;
				// get the t-closest nodes from front.
				// pi that are t-closer with respect to the query
				for (Relationship rel : front.getObject().getRelationships(
						RelTypes.NN, Direction.OUTGOING)) {

					Node pi = rel.getOtherNode(front.getObject());
					short piAndFront = (Short) rel.getProperty(super.PROP_VAL);
					// estimate piAndQ in order to avoid some computations.
					// |d(front,q) - d(front,pi)| <= d(pi,q)
					short estimation = (short) Math.abs(frontAndQ - piAndFront);
					//if (tCloser(estimation, frontAndQ)) {
						if (visited.add(pi)) {
							stats.incSmapCount();
							short piAndQ = lInf(b.getSmapVector(), pi);
							assert estimation <= piAndQ : "Est: " + estimation
									+ " piQ " + piAndQ;
							stats.incExtra("lInf");
							if (tCloser(piAndQ, frontAndQ)) {
								// we add the element to the queue

								searchQueue.add(pi.getId(), pi, piAndQ);
								stats.incExtra("Enqueued");
							}
						}
					//}
					i++;
				}
				// assert i <= super.localk :
				// "Relations must be always maller than localk: "
				// + i;
			}
			txn.success();
			stats.addExtraStats("enqueuedRemaining", searchQueue.getSize());
		} finally {
			txn.finish();
		}
	}

	private short lInf(Node i, Node j) {
		return BucketObjectShort.lInf((short[]) i.getProperty(PROP_SMAP),
				(short[]) j.getProperty(PROP_SMAP));
	}

	private short lInf(short[] center, Node j) {
		short[] other = (short[]) j.getProperty(PROP_SMAP);
		return BucketObjectShort.lInf(center, other);
	}

	/**
	 * An element p_i is t-closer to q with respect to p if d(pi,q) <= t * d(p
	 * ,q)
	 * 
	 * @param pi
	 * @param q
	 * @param respect
	 * @return
	 */
	private boolean tCloser(short piAndQDistance, short pAndQDistance) {
		return piAndQDistance <= super.t * pAndQDistance;
	}

	private short[] getSmapVector(Node n) {
		return (short[]) n.getProperty(PROP_SMAP);
	}

	private class Evaluator implements StopEvaluator, ReturnableEvaluator {

		private OBQueryShort<O> q;
		private BucketObjectShort b;

		public Evaluator(BucketObjectShort b, OBQueryShort<O> q) {
			super();
			this.b = b;
			this.q = q;
		}

		@Override
		public boolean isStopNode(TraversalPosition currentPos) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean isReturnableNode(TraversalPosition currentPos) {
			// TODO Auto-generated method stub
			return false;
		}

	}

	protected void updateSeeds(BucketObjectShort b, O object, long id) {
		short[] smap = b.getSmapVector();
		short smallest = Short.MAX_VALUE;
		int smallestIndex = -1;
		int i = 0;
		while (i < smap.length) {
			if (smap[i] < smallest) {
				smallestIndex = i;
				smallest = smap[i];
			}
			i++;
		}
		if (this.iDistanceSeeds[smallestIndex][smallest] == -1) {
			this.iDistanceSeeds[smallestIndex][smallest] = id;
		}

	}

	public OperationStatus exists(O object) throws OBException,
			IllegalAccessException, InstantiationException {

		//
		OperationStatus res = new OperationStatus();
		res.setStatus(Status.NOT_EXISTS);
		BucketObjectShort b = this.getBucket(object);
		Transaction tx = neo.beginTx();
		try {
			byte[] code = getAddress(b);
			long nid = super.getNodeId(code);
			if (nid != -1) {
				Node n = neo.getNodeById(nid);
				long[] ids = (long[]) n.getProperty(PROP_IDS);
				//
				for (long id : ids) {
					// the objects have the same smap vector, just
					// compare them to see if any has distance 0.
					O o = getObject(id);
					if (o.distance(object) == 0) {
						res.setStatus(Status.EXISTS);
						res.setId(id);
						break;
					}
				}

			}
			tx.success();
		} finally {
			tx.finish();
		}
		return res;
	}

	@Override
	public void searchOB(O object, short r, Filter<O> filter,
			OBPriorityQueueShort<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		// TODO Auto-generated method stub

	}

	@Override
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
			int[] boxes) throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		// TODO Auto-generated method stub

	}

}
