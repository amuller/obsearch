package net.obsearch.index.idistance;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

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
import net.obsearch.index.bucket.BucketContainerShort;
import net.obsearch.index.bucket.BucketObjectShort;
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
		IntegerHolder smapCount = new IntegerHolder(0);
		short lastRange = r; // keep track of the last used range.
		for (DimensionShort s : smap) {
			short center = s.getValue();
			short low = q.getLow()[s.getOrder()];
			short high = q.getHigh()[s.getOrder()];
			byte[] centerKey = buildKey(s.getOrder(), center);
			byte[] lowKey = buildKey(s.getOrder(), low);
			byte[] highKey = buildKey(s.getOrder(), high);
			CloseIterator<TupleBytes> itRight = Buckets.processRange(centerKey,
					highKey);
			CloseIterator<TupleBytes> itLeft = Buckets.processRangeReverse(
					lowKey, centerKey);
			if (itLeft.hasNext()) {
				itLeft.next();// right has the same element!
			}
			
			// if we should continue to the right or left
			// (if the range was reduced we have to recalculate our current
			// position.
			boolean continueRight = true;
			boolean continueLeft = true;
			while ( (itRight.hasNext() && continueRight ) || ( itLeft.hasNext() && continueLeft)
					) {
				if (itRight.hasNext() && continueRight) {
					TupleBytes t = itRight.next();
					// make sure we are within the key limits.
					if (comp.compare(t.getKey(), highKey) > 0) {
						continueRight = false;
					} else {
						BucketContainerShort<O> bt = this
								.instantiateBucketContainer(t.getValue(), null);
						stats
								.incDistanceCount(bt.searchSorted(q, b,
										smapCount));
						stats.incBucketsRead();
						stats.incDataRead(bt.getBytes().array().length);
						// update ranges
						if (q.updatedRange(lastRange)) {
							low = q.getLow()[s.getOrder()];
							high = q.getHigh()[s.getOrder()];
							lowKey = buildKey(s.getOrder(), low);
							highKey = buildKey(s.getOrder(), high);
							lastRange = q.getDistance();					
					}
					}
				}

				if (itLeft.hasNext() && continueLeft) {
					TupleBytes t = itLeft.next();
					if (comp.compare(t.getKey(), lowKey) < 0) {
						continueLeft = false;
					} else {
						BucketContainerShort<O> bt = this
								.instantiateBucketContainer(t.getValue(), null);
						stats
								.incDistanceCount(bt.searchSorted(q, b,
										smapCount));
						stats.incDataRead(bt.getBytes().array().length);
						stats.incBucketsRead();
						if (q.updatedRange(lastRange)) {
							low = q.getLow()[s.getOrder()];
							high = q.getHigh()[s.getOrder()];
							lowKey = buildKey(s.getOrder(), low);
							highKey = buildKey(s.getOrder(), high);
							lastRange = q.getDistance();
						}
					}
				}

				

			}
			itRight.closeCursor();
			itLeft.closeCursor();
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
	
	
}
