package net.obsearch.index.dprime.impl;

import hep.aida.bin.QuantileBin1D;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import net.obsearch.Index;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
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
import net.obsearch.index.dprime.AbstractDPrimeIndex;
import net.obsearch.index.dprime.BinaryTrie;
import net.obsearch.index.utils.IntegerHolder;
import net.obsearch.index.utils.medians.MedianCalculatorShort;
import net.obsearch.ob.OBShort;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.utils.bytes.ByteConversion;

import org.apache.log4j.Logger;

import cern.colt.list.LongArrayList;
import cern.colt.list.ShortArrayList;

import com.sleepycat.je.DatabaseException;

public final class DPrimeIndexShort<O extends OBShort>
		extends
		AbstractDPrimeIndex<O, BucketObjectShort<O>, OBQueryShort<O>, BucketContainerShort<O>>
		implements IndexShort<O> {

	/**
	 * Logger.
	 */
	private static final transient Logger logger = Logger
			.getLogger(DPrimeIndexShort.class);

	public int hackOne = 5;

	/**
	 * Median data for each level and pivot.
	 */
	private short[] median;

	/**
	 * Obtain max distance per pivot... hack. Remove this in the future.
	 */
	private short maxDistance;

	/**
	 * For each pivot, we have here how many objects fall in distance x. Max:
	 * right of the median Min: left of the median
	 */
	private int[][] distanceDistribution;

	protected float[][] normalizedProbs;
	
	/**
	 * Hack used to calculate the medians.
	 */
	private short expectedMaxDistance = Short.MAX_VALUE;

	/**
	 * Creates a new DIndex for shorts
	 * 
	 * @param fact
	 *            Storage factory to use
	 * @param pivotCount
	 *            number of pivots to use.
	 * @param pivotSelector
	 *            Pivot acceptance criteria.
	 * @param type
	 *            The type of objects to use (needed to create new instances)
	 * @param nextLevelThreshold
	 *            threshold used to reduce the number of pivots per level.
	 * @param p
	 *            P parameter of D-Index.
	 * @throws OBStorageException
	 *             If something goes wrong with the storage device.
	 * @throws OBException
	 *             If some other exception occurs.
	 */
	public DPrimeIndexShort(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);

	}
	
	public void setExpectedMaxDistance(short maxDistance){
		logger.info("Max distance");
		this.expectedMaxDistance = maxDistance;
	}

	protected BucketObjectShort getBucket(O object, short p) throws OBException {

		BucketObjectShort res = new BucketObjectShort(convertTuple(object), -1L);
		return res;
	}

	private short[] convertTuple(O object) throws OBException {
		int i = 0;
		O[] piv = super.pivots;
		short[] smapVector = new short[piv.length];
		while (i < piv.length) {
			short distance = piv[i].distance(object);
			smapVector[i] = distance;
			i++;
		}
		return smapVector;
	}
	
	

	@Override
	protected int primitiveDataTypeSize() {
		return this.getPivotCount() * ByteConstants.Short.getSize()
				+ Index.ID_SIZE;
	}

	/**
	 * Calculate the bucket id of the given bucket.
	 * 
	 * @param b
	 *            The bucket we will process.
	 * @return the bucket id of b.
	 */
	protected long getBucketId(BucketObjectShort b) {
		int i = 0;
		O[] piv = super.pivots;
		short[] medians = median;
		short[] smapVector = b.getSmapVector();
		long bucketId = 0;
		while (i < piv.length) {
			short distance = smapVector[i];
			int r = bps(medians[i], distance);
			if (r == 1) {
				bucketId = bucketId | super.masks[i];
			}
			i++;
		}
		return bucketId;
	}

	@Override
	protected BucketObjectShort getBucket(O object) throws OBException {
		return getBucket(object, (short) 0);
	}

	public boolean intersects(O object, short r, int box)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}

	public Iterator<Long> intersectingBoxes(O object, short r)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException {
		throw new UnsupportedOperationException();
	}

	/**
	 * Bps function. Returns 0 if d(o,p) <= median - p . Returns 1 if d(o,p) >
	 * median +p. Returns 2 otherwise.
	 * 
	 * @param median
	 *            Median obtained for the given pivot.
	 * @param distance
	 *            Distance of the pivot and the object we are processing
	 * @return Returns 0 if d(o,p) <= median - p . Returns 1 if d(o,p) > median
	 *         +p. Returns 2 otherwise.
	 */
	private int bps(short median, short distance) {
		if (distance <= median) {
			return 0;
		} else {
			return 1;
		}
	}

	private int bpsRange(short median, short distance, short range) {
		boolean a = distance - range <= median;
		boolean b = distance + range >= median;
		if (a && b) {
			return 2;
		} else if (a) {
			return 0;
		} else if (b) {
			return 1;
		} else {
			assert false;
			return -1;
		}
	}

	protected void calculateMedians(LongArrayList elementsSource)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		int i = 0;
		O[] pivots = super.pivots;

		int max;
		if (elementsSource == null) {
			max = (int) A.size();
		} else {
			max = elementsSource.size();
		}
		
		i = 0;
		MedianCalculatorShort[]  medianHolders = new MedianCalculatorShort[getPivotCount()];  
		//ShortArrayList[] shorts = new ShortArrayList[getPivotCount()];
		while(i < getPivotCount()){
			medianHolders[i] = new MedianCalculatorShort(this.expectedMaxDistance);
			//shorts[i] = new ShortArrayList(max);
			i++;
		}

		
		// init median arrays.

		logger.debug("Calculating medians. max: " + max);
		int cx = 0;
		while (cx < max) {
			O o = getObjectFreeze(cx, elementsSource);
			if(cx % 100000 == 0){
				logger.info("Calculating medians: " + cx);
			}
			median = new short[getPivotCount()];
			i = 0;
			while (i < getPivotCount()) {
				O p = pivots[i];
				short d = p.distance(o);
				if (maxDistance < d) {
					maxDistance = d;
				}
				medianHolders[i].add(d);
				//shorts[i].add(d);
				i++;
			}

			cx++;
		}
		i = 0;
		for(MedianCalculatorShort q : medianHolders){
			median[i] = (short)q.median();
			logger.info("Median" + q.toString());
			//logger.info("Real: " + median(shorts[i]));
			i++;
		}
		logger.info("max distance: " + maxDistance);
		maxDistance++;
		assert i > 0;
		if (logger.isDebugEnabled()) {
			logger.debug("Found medians: " + Arrays.toString(median));
		}

		assert getPivotCount() == median.length : "Piv: " + getPivotCount()
				+ " Med: " + median.length;
		this.distanceDistribution = new int[getPivotCount()][maxDistance + 1];
	}

	/**
	 * Updates probability information.
	 * 
	 * @param b
	 */
	protected void updateProbabilities(BucketObjectShort b) {
		int i = 0;
		while (i < getPivotCount()) {

			this.distanceDistribution[i][b.getSmapVector()[i]]++;

			i++;
		}

	}

	protected void normalizeProbs() throws OBStorageException {
		normalizedProbs = new float[getPivotCount()][this.maxDistance + 1];
		long total = A.size();
		int i = 0;
		while (i < getPivotCount()) {

			// calculate accum distances
			int m = this.median[i];
			// initialize center
			normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total);
			m--;
			// process the left side of the median
			while (m >= 0) {
				normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total)
						+ normalizedProbs[i][m + 1];
				m--;
			}
			// process the right side of the median

			m = this.median[i] + 1; // do not include the median.
			if (m < normalizedProbs[i].length) {
				normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total);
				m++;
			}
			while (m < normalizedProbs[i].length) {
				normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total)
						+ normalizedProbs[i][m - 1];
				m++;
			}
			i++;
		}
		logger.debug("Probs normalized");
	}

	private short median(ShortArrayList medianData) {
		medianData.sort();
		return medianData.get(medianData.size() / 2);
	}

	@Override
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
			int[] boxes) throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();

	}

	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {

		BucketObjectShort b = getBucket(object);
		OBQueryShort<O> q = new OBQueryShort<O>(object, r, result, b
				.getSmapVector());

		stats.incQueryCount();
		long originalBucketId = this.getBucketId(b);
		this.s(originalBucketId, b, q, false, originalBucketId);

		doIt1(b, q, 0, 0, originalBucketId, filter); // heu 1 + heu 2

		/*
		 * while (i < pivots.size()) {// search through all the levels. b =
		 * getBucket(object, i, (short) 0); assert !b.isExclusionBucket();
		 * BucketContainerShort < O > bc = super.bucketContainerCache
		 * .get(super.getBucketStorageId(b)); bc.search(q, b); i++; } //
		 * finally, search the exclusion bucket :) BucketContainerShort < O > bc
		 * = super.bucketContainerCache .get(super.exclusionBucketId);
		 * bc.search(q, b);
		 */
	}

	// assuming that this query goes beyond the median.
	private float calculateZero(BucketObjectShort b, OBQueryShort<O> q,
			int pivotIndex) {
		short base = (short) Math.max(q.getLow()[pivotIndex], 0);
		return this.normalizedProbs[pivotIndex][base];
	}

	// assuming that this query goes beyond the median.
	private float calculateOne(BucketObjectShort b, OBQueryShort<O> q,
			int pivotIndex) {
		short top = (short) Math.min(q.getHigh()[pivotIndex], maxDistance);
		return this.normalizedProbs[pivotIndex][top];
	}

	/**
	 * Does the match for the given index for the given pivot.
	 * 
	 * @param b
	 * @param q
	 * @param pivotIndex
	 */
	private void doIt1(BucketObjectShort b, OBQueryShort<O> q, int pivotIndex,
			long block, long originalBucketId, BinaryTrie f) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		if (pivotIndex < getPivotCount()) {
			int r = bpsRange(median[pivotIndex], b.getSmapVector()[pivotIndex],
					q.getDistance());
			if (r == 2) { // if we have to do both
				if (calculateOne(b, q, pivotIndex) > calculateZero(b, q,
						pivotIndex)) {
					// do 1 first
					long newBlock = block | super.masks[pivotIndex];
					if (f.isOne()) {
						doIt1(b, q, pivotIndex + 1, newBlock, originalBucketId, f.getOne());
					}
					r = bpsRange(median[pivotIndex],
							b.getSmapVector()[pivotIndex], q.getDistance());
					if ((r == 2 || r == 0)
							&& f.isZero()) {
						doIt1(b, q, pivotIndex + 1, block, originalBucketId, f.getZero());
					}

				} else {
					// 0 first
					if (f.isZero()) {
						doIt1(b, q, pivotIndex + 1, block, originalBucketId, f.getZero());
					}
					r = bpsRange(median[pivotIndex],
							b.getSmapVector()[pivotIndex], q.getDistance());
					long newBlock = block | super.masks[pivotIndex];
					if ((r == 2 || r == 1)
							&& f.isOne()) {

						doIt1(b, q, pivotIndex + 1, newBlock, originalBucketId, f.getOne());
					}
				}

			} else { // only one of the sides is selected
				// zero first
				if (r == 0 && f.isZero()) {
					doIt1(b, q, pivotIndex + 1, block, originalBucketId, f.getZero());
				} else { // mask 1
					long newBlock = block | super.masks[pivotIndex];
					if (f.isOne()) {
						doIt1(b, q, pivotIndex + 1, newBlock, originalBucketId, f.getOne());
					}
				}
			}

		} else {

			s(block, b, q, true, originalBucketId);

		}
	}

	/**
	 * @param block
	 *            The block to be processed
	 * @param b
	 *            The block of the object
	 * @param q
	 *            The query
	 * @param ignoreSameBlocks
	 *            if true, if block == b.getBucket() nothing happens.
	 * @throws NotFrozenEx
	 *             `ception
	 * @throws DatabaseException
	 * @throws InstantiationException
	 * @throws IllegalIdException
	 * @throws IllegalAccessException
	 * @throws OutOfRangeException
	 * @throws OBException
	 */
	private void s(long block, BucketObjectShort b, OBQueryShort<O> q,
			boolean ignoreSameBlocks, long originalBucketId)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {

		if (super.filter.containsInv(block)) {
			if (!ignoreSameBlocks || block != originalBucketId) {
				// we have finished

				BucketContainerShort<O> bc = instantiateBucketContainer(block);
				stats.incBucketsRead();
				// assert bc.size() != 0; // this should not happen.

				IntegerHolder data = new IntegerHolder(0);
				IntegerHolder h = new IntegerHolder(0);
				stats.incDistanceCount(bc.search(q, b, h, data, null));
				stats.incDataRead(data.getValue());
				// stats.incDistanceCount(bc.search(q, b));
				stats.incSmapCount(h.getValue());
				
			}
		}

	}

	/*@Override
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
	}*/

	@Override
	protected byte[] getAddress(BucketObjectShort bucket) {
		return longToBytes(this.getBucketId(bucket));
	}

	
	@Override
	protected BucketContainerShort<O> instantiateBucketContainer(
			ByteBuffer data, byte[] address) {
		return new BucketContainerShort<O>(this, getPivotCount(), Buckets,
				address, Math.abs(Arrays.hashCode(address)) % this.getPivotCount());
	}

	protected BucketContainerShort<O> instantiateBucketContainer(long id) {
		return instantiateBucketContainer(null, longToBytes(id));
	}

	private byte[] longToBytes(long id) {
		ByteBuffer b = ByteConversion.createByteBuffer(ByteConstants.Long
				.getSize());
		b.putLong(id);
		return b.array();
	}

	@Override
	public void searchOB(O object, short r, Filter<O> filter,
			OBPriorityQueueShort<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		throw new UnsupportedOperationException();

	}
	
	

}
