package net.obsearch.index.dprime;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import net.obsearch.AbstractOBPriorityQueue;
import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCache;
import net.obsearch.cache.OBCacheLoader;
import net.obsearch.cache.OBCacheLoaderLong;
import net.obsearch.cache.OBCacheLong;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.bucket.SimpleBloomFilter;
import net.obsearch.index.pivot.AbstractPivotOBIndex;
import net.obsearch.index.utils.OBFactory;
import net.obsearch.index.utils.StatsUtil;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreInt;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleLong;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

/**
 * AbstractDIndex contains functionality common to specializations of the
 * D-Index for primitive types.
 * 
 * @param < O >
 *            The OB object that will be stored.
 * @param < B >
 *            The Object bucket that will be used.
 * @param < Q >
 *            The query object type.
 * @param < BC>
 *            The Bucket container.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractDPrimeIndex<O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractPivotOBIndex<O> {
	/**
	 * Logger.
	 */
	private static final transient Logger logger = Logger
			.getLogger(AbstractDPrimeIndex.class);

	/**
	 * Filter used to avoid unnecessary block accesses.
	 */
	// protected ArrayList< SimpleBloomFilter<Long>> filter;
	protected ArrayList<HashSet<Long>> filter;

	/**
	 * We store the buckets in this storage device.
	 */
	protected transient OBStoreLong Buckets;

	/**
	 * Cache used for storing recently accessed Buckets.
	 */
	protected transient OBCacheLong<BC> bucketContainerCache;

	/**
	 * Masks used to speedup the generation of hash table codes.
	 */
	protected long[] masks;

	/**
	 * Accumulated pivots per level (2 ^ pivots per level) so that we can
	 * quickly compute the correct storage device
	 */
	private long[] accum;

	/**
	 * Initializes this abstract class.
	 * 
	 * @param fact
	 *            The factory that will be used to create storage devices.
	 * @param pivotCount
	 *            # of Pivots that will be used.
	 * @param pivotSelector
	 *            The pivot selection mechanism to be used. (64 pivots can
	 *            create 2^64 different buckets, so we have limited the # of
	 *            pivots available. If you need more pivots, then you would have
	 *            to use the Smapped P+Tree (PPTree*).
	 * @param type
	 *            The type that will be stored in this index.
	 * @param nextLevelThreshold
	 *            How many pivots will be used on each level of the hash table.
	 * @throws OBStorageException
	 *             if a storage device could not be created for storing objects.
	 */
	protected AbstractDPrimeIndex(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);		
	}

	public void init(OBStoreFactory fact) throws OBStorageException, OBException, InstantiationException, IllegalAccessException {
		super.init(fact);
		this.Buckets = fact.createOBStoreLong("Buckets", false, false);
		if (this.bucketContainerCache == null) {
			this.bucketContainerCache = new OBCacheLong<BC>(new BucketLoader());
		}
		
		// initialize the masks;
		int i = 0;
		long mask = 1;
		this.masks = new long[64];
		while (i < 64) {
			logger.debug(Long.toBinaryString(mask));
			masks[i] = mask;
			mask = mask << 1;
			i++;
		}

	}

	
	/**
	 * Subclasses must call this method after they have closed the storage
	 * devices they created.
	 * 
	 * @throws OBException
	 */
	public void close() throws OBException {
		this.Buckets.close();
		super.close();
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get n pivots.
		// ask the bucket for each object and insert those who are not excluded.
		// repeat iteratively with the objects that could not be inserted until
		// the remaining
		// objects are small enough.
		super.freeze();

		// initialize bloom filter
		int i = 0;
		// this.filter = new ArrayList<SimpleBloomFilter<Long>>();
		this.filter = new ArrayList<HashSet<Long>>();

		while (i < getPivotCount()) {
			// filter.add(new SimpleBloomFilter<Long>(i * 1000,
			// (int)Math.pow(2, i)));
			filter.add(new HashSet<Long>());
			i++;
		}

		// the initial list of object from which we will generate the pivots
		LongArrayList elementsSource = null;
		// After generating the pivots, we put here the objects that fell
		// into the exclusion bucket.

		int maxBucketSize = 0;

		int insertedObjects = 0;
		boolean cont = true;

		// calculate medians required to be able to use the bps
		// function.
		calculateMedians(elementsSource);
		i = 0;
		int max;
		if (elementsSource == null) {
			max = (int) A.size();
		} else {
			max = elementsSource.size();
		}

		HashMap<Long, java.util.List<B>> buckets = new HashMap<Long, java.util.List<B>>();

		while (i < max) {
			O o = getObjectFreeze(i, elementsSource);
			B b = getBucket(o);
			updateProbabilities(b);
			b.setId(idMap(i, elementsSource));
			// insertBucket(b, o);
			long bucketId = getBucketId(b);
			java.util.List<B> l = buckets.get(bucketId);
			if (l == null) {
				l = new LinkedList<B>();
				buckets.put(bucketId, l);
			}
			l.add(b);
			if (logger.isDebugEnabled() && (i % 10000 == 0)) {
				logger.debug("Inserted: " + i);
				bucketStats();
			}
			insertedObjects++;
			// BC bc = this.bucketContainerCache.get(getBucketStorageId(b));
			// assert bc.exists(b, o).getStatus() ==
			// OperationStatus.Status.EXISTS;
			if (maxBucketSize < l.size()) {
				maxBucketSize = l.size();
			}

			i++;
		}

		normalizeProbs();

		// insert the buckets.
		for (java.util.List<B> l : buckets.values()) {
			insertBuckets(l);
		}
		bucketStats();

		logger.debug("Max bucket size: " + maxBucketSize);
		logger.debug("Bucket count: " + A.size());

		bucketStats();

	}

	
	/**
	 * Return the bucket id of the given bucket
	 * @param b The bucket that will be processed.
	 * @return The bucket id of b.
	 */
	protected abstract long getBucketId(B b);
	
	private void bucketStats() throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {

		CloseIterator<TupleLong> it = Buckets.processRange(Long.MIN_VALUE,
				Long.MAX_VALUE);
		StaticBin1D s = new StaticBin1D();
		while (it.hasNext()) {
			TupleLong t = it.next();
			BC bc = this.bucketContainerCache.get(t.getKey());
			s.add(bc.size());
		} // add exlucion
		logger.info(StatsUtil.mightyIOStats("Bucket distribution", s));
		it.closeCursor();
	}

	/**
	 * Updates probability information.
	 * 
	 * @param b
	 */
	protected abstract void updateProbabilities(B b);

	protected abstract void normalizeProbs() throws OBStorageException;

	/**
	 * Calculate median values for pivots of level i based on the
	 * elementsSource. If elementsSource == null, all the objects in the DB are
	 * used.
	 * 
	 * @param level
	 *            The level that will be processed.
	 * @param elementsSource
	 *            The objects that will be processed to generate median data
	 *            information.
	 * @throws OBStorageException
	 *             if something goes wrong with the storage device.
	 */
	protected abstract void calculateMedians(LongArrayList elementsSource)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException;

	/**
	 * Stores the given bucket b into the {@link #Buckets} storage device. The
	 * given bucket b should have been returned by {@link #getBucket(OB, int)}
	 * 
	 * @param b
	 *            The bucket in which we will insert the object.
	 * @param object
	 *            The object to insert.
	 * @return A OperationStatus object with the new id of the object if the
	 *         object was inserted successfully.
	 * @throws OBStorageException
	 */
	private OperationStatus insertBucket(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get the bucket id.
		long bucketId = getBucketId(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.
		BC bc = this.bucketContainerCache.get(bucketId);
		if (bc == null) { // it was just created for the first
			// time.
			bc = instantiateBucketContainer(null);
			bc.setPivots(getPivotCount());
			updateFilters(bucketId);
		} else {
			assert bc.getPivots() == b.getPivotSize() : " Pivot size: "
					+ bc.getPivots() + " b pivot size: " + b.getPivotSize();
		}
		OperationStatus res = new OperationStatus();
		synchronized (bc) {
			res = bc.exists(b, object);
			if (res.getStatus() != Status.EXISTS) {

				assert bc.getPivots() == b.getPivotSize() : "BC: "
						+ bc.getPivots() + " b: " + b.getPivotSize();
				bc.insert(b);
				putBucket(bucketId, bc);
				res.setStatus(Status.OK);
			}
		}
		return res;
	}

	private OperationStatus insertBuckets(java.util.List<B> b)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {

		// get the bucket id.
		long bucketId = getBucketId(b.get(0));
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.
		BC bc = this.bucketContainerCache.get(bucketId);
		if (bc == null) { // it was just created for the first
			// time.
			bc = instantiateBucketContainer(null);
			bc.bulkInsert(b);
			bc.setPivots(super.getPivotCount());
			updateFilters(bucketId);
		} else {
			assert bc.getPivots() == b.get(0).getPivotSize() : " Pivot size: "
					+ bc.getPivots() + " b pivot size: "
					+ b.get(0).getPivotSize();
		}
		OperationStatus res = new OperationStatus();

		bc.bulkInsert(b);
		putBucket(bucketId, bc);
		res.setStatus(Status.OK);

		return res;
	}

	/**
	 * @param bucket
	 */
	private void putBucket(long bucketId, BC bc) throws OBStorageException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {

		Buckets.put(bucketId, bc.getBytes());
	}

	/** updates the bucket identification filters */
	private void updateFilters(long x) {
		String s = Long.toBinaryString(x);
		int max = s.length();
		int i = s.length() - 1;
		int cx = 0;
		while (i >= 0) {
			long j = Long.parseLong(s.substring(i, max), 2);
			// if(! filter.get(cx).contains(j)){
			filter.get(cx).add(j);
			// }
			i--;
			cx++;
		}
		// add the long to the rest of the layers.
		while (max < getPivotCount()) {
			filter.get(max).add(x);
			max++;
		}
	}

	/**
	 * If elementSource == null returns id, otherwise it returns
	 * elementSource[id]
	 * 
	 * @return
	 */
	private long idMap(long id, LongArrayList elementSource) throws OBException {
		OBAsserts.chkAssert(id <= Integer.MAX_VALUE,
				"id for this stage must be smaller than 2^32");
		if (elementSource == null) {
			return id;
		} else {
			return elementSource.get((int) id);
		}
	}

	/**
	 * Auxiliary function used in freeze to get objects directly from the DB, or
	 * by using an array of object ids.
	 */
	protected O getObjectFreeze(long id, LongArrayList elementSource)
			throws IllegalIdException, IllegalAccessException,
			InstantiationException, OutOfRangeException, OBException {

		return getObject(idMap(id, elementSource));

	}

	/**
	 * Returns the bucket information for the given object.
	 * 
	 * @param object
	 *            The object that will be calculated
	 * @return The bucket information for the given object.
	 */
	protected abstract B getBucket(O object) throws OBException;

	@Override
	public OperationStatus insertAux(long id, O object) throws OBException,
			IllegalAccessException, InstantiationException {

		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
		B b = getBucket(object);
		b.setId(id);
		res = this.insertBucket(b, object);
		res.setId(id);

		return res;
	}

	public OperationStatus deleteAux(O object) throws OBException,
			IllegalAccessException, InstantiationException {
		
		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
		B b = getBucket(object);
		long bucketId = getBucketId(b);
		BC bc = this.bucketContainerCache.get(bucketId);
		if (bc == null) { 
			res.setStatus(Status.NOT_EXISTS);
		}else{
			res = bc.delete(b, object);
		}
		return res;
	}

	



	/**
	 * Get a bucket container fromt he given data.
	 * 
	 * @param data
	 *            The data from which the bucket container will be loaded.
	 * @return A new bucket container ready to be used.
	 */
	protected abstract BC instantiateBucketContainer(ByteBuffer data);

	private class BucketLoader implements OBCacheLoaderLong<BC> {

		public long getDBSize() throws OBStorageException {
			return (int) A.size();
		}

		public BC loadObject(long i) throws OutOfRangeException, OBException,
				InstantiationException, IllegalAccessException {
			ByteBuffer data = Buckets.getValue(i);
			if (data == null) {
				return null;
			} else {
				return instantiateBucketContainer(data);
			}

		}

	}

}
