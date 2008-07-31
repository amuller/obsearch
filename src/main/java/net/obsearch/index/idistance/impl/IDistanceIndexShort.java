package net.obsearch.index.idistance.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCache;
import net.obsearch.dimension.DimensionShort;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.IndexShort;

import net.obsearch.index.bucket.impl.BucketContainerShort;
import net.obsearch.index.bucket.impl.BucketObjectShort;
import net.obsearch.index.idistance.AbstractIDistanceIndex;
import net.obsearch.index.utils.ByteArrayComparator;
import net.obsearch.index.utils.IntegerHolder;
import net.obsearch.ob.OBShort;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStoreInt;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteBufferFactoryConversion;

public class IDistanceIndexShort<O extends OBShort>
		extends
		AbstractIDistanceIndex<O, BucketObjectShort, OBQueryShort<O>, BucketContainerShort<O>>
		implements IndexShort<O> {

	private static ByteArrayComparator comp = new ByteArrayComparator();

	/**
	 * min max values per pivot.
	 */
	private OBStoreInt minMax;

	private OBCache minMaxCache;

	/**
	 * Creates a new iDistance index.
	 * 
	 * @param type
	 *            Type of object to store.
	 * @param pivotSelector
	 *            Pivot selection procedure that will be used.
	 * @param pivotCount
	 *            Number of pivots used to initialize the index.
	 * @throws OBStorageException
	 * @throws OBException
	 */
	public IDistanceIndexShort(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
	}

	@Override
	public OperationStatus exists(O object) throws OBException,
			IllegalAccessException, InstantiationException {
		OBPriorityQueueShort<O> result = new OBPriorityQueueShort<O>((byte) 1);
		searchOB(object, (short) 0, result);
		OperationStatus res = new OperationStatus();
		res.setStatus(Status.NOT_EXISTS);
		if (result.getSize() == 1) {
			Iterator<OBResultShort<O>> it = result.iterator();
			OBResultShort<O> r = it.next();
			if (r.getObject().equals(object)) {
				res.setId(r.getId());
				res.setStatus(Status.EXISTS);
			}
		}
		return res;
	}

	@Override
	protected byte[] getAddress(BucketObjectShort bucket) {

		short[] smap = bucket.getSmapVector();
		int i = 0;
		int iMin = -1;
		short minValue = Short.MAX_VALUE;
		while (i < smap.length) {
			if (smap[i] < minValue) {
				iMin = i;
				minValue = smap[i];
			}
			i++;
		}

		return buildKey(iMin, minValue);
	}

	private byte[] buildKey(int i, short value) {
		ByteBuffer buf = ByteBufferFactoryConversion.createByteBuffer(0, 1, 1,
				0, 0, 0);
		buf.put(fact.serializeInt(i));
		buf.put(fact.serializeShort(value));
		return buf.array();
	}

	@Override
	protected BucketObjectShort getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		short[] smapTuple = DimensionShort.getPrimitiveTuple(super.pivots,
				object);
		return new BucketObjectShort(smapTuple, -1);
	}

	@Override
	protected BucketContainerShort<O> instantiateBucketContainer(
			ByteBuffer data, byte[] address) {
		return new BucketContainerShort<O>(this, data, getPivotCount());
	}

	@Override
	public Iterator<Long> intersectingBoxes(O object, short r)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean intersects(O object, short r, int box)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		BucketObjectShort b = getBucket(object);
		OBQueryShort<O> q = new OBQueryShort<O>(object, r, result, b
				.getSmapVector());
		DimensionShort[] smap = DimensionShort.transformPrimitiveTuple(b
				.getSmapVector());
		Arrays.sort(smap);
		LinkedList<DimensionProcessor> ll = new LinkedList<DimensionProcessor>();
		for (DimensionShort s : smap) {
			ll.add(new DimensionProcessor(b, q, s));
		}
		IntegerHolder smapCount = new IntegerHolder(0);
		while (ll.size() > 0) {
			ListIterator<DimensionProcessor> pit = ll.listIterator();
			while (pit.hasNext()) {
				DimensionProcessor p = pit.next();
				if (p.hasNext()) {
					p.doIt(smapCount);
				} else {
					p.close();
					pit.remove();
				}
			}
			//if(q.getResult().getSize() == q.getResult().getK()){
				// do something when k is found.
			//}
		}

		stats.incSmapCount(smapCount.getValue());
	}

	@Override
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
			int[] boxes) throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Process an entire dimension little by little.
	 * 
	 * @author Arnoldo Muller Molina
	 * 
	 */
	private class DimensionProcessor {

		private CloseIterator<TupleBytes> itRight;
		private CloseIterator<TupleBytes> itLeft;
		boolean continueRight = true;
		boolean continueLeft = true;
		boolean iteration = true;// iteration id.
		BucketObjectShort b;
		private OBQueryShort<O> q;
		DimensionShort s;
		byte[] centerKey;
		byte[] lowKey;
		byte[] highKey;
		short lastRange;

		public DimensionProcessor(BucketObjectShort b, OBQueryShort<O> q,
				DimensionShort s) throws OBStorageException {
			super();
			this.b = b;
			this.q = q;
			this.s = s;
			updateHighLow();
			itRight = Buckets.processRange(centerKey, highKey);
			itLeft = Buckets.processRangeReverse(lowKey, centerKey);
			if(itLeft.hasNext()){
				itLeft.next();
			}
		}

		/**
		 * Update high and low intervals.
		 */
		private void updateHighLow() {
			short center = s.getValue();
			short low = q.getLow()[s.getOrder()];
			short high = q.getHigh()[s.getOrder()];
			centerKey = buildKey(s.getOrder(), center);
			lowKey = buildKey(s.getOrder(), low);
			highKey = buildKey(s.getOrder(), high);
			this.lastRange = q.getDistance();
		}

		public void close() throws OBException {
			itRight.closeCursor();
			itLeft.closeCursor();
		}

		public boolean hasNext() {
			return (itRight.hasNext() && continueRight)
					|| (itLeft.hasNext() && continueLeft);
		}

		/**
		 * Perform one matching iteration.
		 * 
		 * @param low
		 * @param high
		 * @return
		 * @throws InstantiationException
		 * @throws OBException
		 * @throws IllegalAccessException
		 * @throws IllegalIdException
		 */
		public void doIt(IntegerHolder smapCount) throws IllegalIdException,
				IllegalAccessException, OBException, InstantiationException {
			if (iteration) {
				if (itRight.hasNext() && continueRight) {
					TupleBytes t = itRight.next();
					// make sure we are within the key limits.
					if (comp.compare(t.getKey(), highKey) > 0) {
						continueRight = false;
					} else {
						BucketContainerShort<O> bt = instantiateBucketContainer(
								t.getValue(), null);
						stats
								.incDistanceCount(bt.searchSorted(q, b,
										smapCount));
						stats.incBucketsRead();
						stats.incDataRead(bt.getBytes().array().length);
						// update ranges
						if (q.updatedRange(lastRange)) {
							updateHighLow();
						}
					}
				}
				iteration = false;
			} else {

				if (itLeft.hasNext() && continueLeft) {
					TupleBytes t = itLeft.next();
					if (comp.compare(t.getKey(), lowKey) < 0) {
						continueLeft = false;
					} else {
						BucketContainerShort<O> bt = instantiateBucketContainer(
								t.getValue(), null);
						stats
								.incDistanceCount(bt.searchSorted(q, b,
										smapCount));
						stats.incDataRead(bt.getBytes().array().length);
						stats.incBucketsRead();
						if (q.updatedRange(lastRange)) {
							updateHighLow();
						}
					}
				}
				iteration = true;
			}
		}

	}

}
