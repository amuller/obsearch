package net.obsearch.index.ghs;

import hep.aida.bin.StaticBin1D;
import it.unimi.dsi.io.InputBitStream;

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import cern.colt.Arrays;

import net.obsearch.AbstractOBResult;
import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.cache.OBCacheByteArray;
import net.obsearch.cache.OBCacheHandlerByteArray;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.dimension.AbstractDimension;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.filter.FilterNonEquals;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.result.OBResultInt;
import net.obsearch.result.OBResultInvertedByte;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2009 Arnoldo Jose Muller Molina

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

/**
 * AbstractSketch64 encapsulates functionality common to a Sketch. The maximum
 * number of bits has been artificially set to 64 bits to optimize performance.
 * If you use less than 64 bits the keys used for the sketch only use the amount
 * of bits the user specifies. Nevertheless the handling of priority queues and
 * the matching itself occurs on longs. It should affect considerably AMD64
 * architectures. If you are using a 32 bits processor or operating system, then
 * this implementation will not be as efficient.
 * 
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractSketch64<O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractBucketIndex<O, B, Q, BC> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractSketch64.class.getName());

	/**
	 * Estimators for k ranges. This object tells us the amount of buckets we
	 * should read for a given k query to match a certain value of error.
	 */
	protected StaticBin1D[] kEstimators;

	/**
	 * Number of samples employed to generate the k estimators.
	 */
	private int sampleSize = 20;

	/**
	 * For a query k we take the kEstimators[k] estimation and return the value
	 * kEstimators[k].mean() + (kEstimators[k].stdDev() * kAlpha)
	 */
	private float kAlpha = 2f; // two standard deviations, 95% precision

	/**
	 * K configuration that will be used by the user.
	 */
	protected int[] userK = new int[]{1, 3, 10, 50};

	/**
	 * The target EP value used in the estimation.
	 */
	private double expectedEP = 0.0;

	private Random r = new Random();

	/**
	 * Number of bits.
	 */
	protected int m;

	/**
	 * A compressed bit set for 64 bits.
	 */
	protected transient CompressedBitSet64 sketchSet;

	/**
	 * Cache used for storing buckets
	 */
	protected transient OBCacheByteArray<BC> bucketCache;

	/**
	 * Only two values per dimension (1, 0)
	 */
	private static final int HEIGHT = 2;

	/**
	 * pivot grid.
	 */
	protected O[][] pivotGrid;

	/**
	 * Distortion stats
	 */
	protected int[][] distortionStats;

	/**
	 * Pivot count for each bucket.
	 */
	protected int bucketPivotCount;

	/**
	 * Pivot selector for the masks.
	 */
	protected IncrementalPivotSelector<O> maskPivotSelector;

	public AbstractSketch64(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int m,
			int bucketPivotCount) throws OBStorageException, OBException {
		super(type, null, 0);
		this.m = m;
		this.bucketPivotCount = bucketPivotCount;
		this.maskPivotSelector = pivotSelector;
		pivotGrid = (O[][]) Array.newInstance(type, m, HEIGHT);
		distortionStats = new int[m][HEIGHT];
	}

	public int getBucketPivotCount() {
		return bucketPivotCount;
	}

	/**
	 * For a query k we take the kEstimators[k] estimation and return the value
	 * kEstimators[k].mean() + (kEstimators[k].stdDev() * kAlpha) This method
	 * sets kAlpha.
	 * 
	 * @param kAlpha
	 *            new kAlpha to set.
	 */
	public void setKAlpha(float kAlpha) {
		this.kAlpha = kAlpha;
	}

	/**
	 * Set the k values used by this index.
	 * 
	 * @param userK
	 */
	public void setMaxK(int[] maxK) {
		this.userK = maxK;
	}

	/**
	 * Set the sample size of the estimation
	 * 
	 * @param size
	 *            the new size
	 */
	public void setSampleSize(int size) {
		this.sampleSize = size;
	}

	/**
	 * Set the expected ep for the k estimation process. We will search for the
	 * number of buckets required to satisfy a value less than ep for a k-nn
	 * query.
	 * 
	 * @param ep
	 *            EP value.
	 * 
	 */
	public void setExpectedEP(double ep) {
		this.expectedEP = ep;
	}

	/**
	 * Calculate the estimators.
	 * 
	 * @throws IllegalIdException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected void calculateEstimators() throws IllegalIdException,
			OBException, IllegalAccessException, InstantiationException {
		maxKEstimation();
	}

	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, InstantiationException, IllegalAccessException {
		super.init(fact);
		// we have to load the masks before the match begins!
		loadMasks();
	}

	protected void initByteArrayBuckets() throws OBException {
		OBStorageConfig conf = new OBStorageConfig();
		conf.setTemp(false);
		conf.setDuplicates(false);
		conf.setBulkMode(!isFrozen());
		conf.setRecordSize(primitiveDataTypeSize());
		this.Buckets = fact.createOBStore("Buckets_byte_array", conf);

	}

	protected BC getBucketContainer(byte[] id) throws OBException,
			InstantiationException, IllegalAccessException {
		// BC bc = instantiateBucketContainer(null, id);
		BC container = this.bucketCache.get(id);
		return container;

	}

	/**
	 * Load the masks from the storage device into memory.
	 * 
	 * @throws OBException
	 */
	protected void loadMasks() throws OBException {
		logger.fine("Loading masks!");
		sketchSet = new CompressedBitSet64();
		CloseIterator<TupleBytes> it = Buckets.processAll();
		// assert Buckets.size() == A.size();
		while (it.hasNext()) {
			TupleBytes t = it.next();
			long bucketId = fact.deSerializeLong(t.getKey());
			sketchSet.add(bucketId);
		}
		it.closeCursor();

		// load the sketch into memory.
		sketchSet.commit();

		logger.info("Compressed Sketch size: " + sketchSet.getBytesSize());

	}

	/**
	 * Sort all masks, and then start the search until the EP is less than some
	 * threshold. Do this for each k.
	 * 
	 * @throws IllegalIdException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private void maxKEstimation() throws IllegalIdException, OBException,
			IllegalAccessException, InstantiationException {
		kEstimators = new StaticBin1D[getMaxK().length];
		logger.fine("Max k estimation");
		int i = 0;
		while (i < kEstimators.length) {
			kEstimators[i] = new StaticBin1D();
			i++;
		}

		List<O> db = getAllObjects();			

		long[] sample = AbstractDimension.select(sampleSize, r, null,
				(Index) this, null);
		O[] sampleSet = getObjects(sample);
		i = 0;
		for (O o : sampleSet) {
			logger.info("Estimating k sample #: " + i + " of " + sampleSize);
			maxKEstimationAux(o, db);
			i++;
		}

		i = 0;
		for (StaticBin1D s : kEstimators) {
			logger.info(" k" + userK[i]);
			logger.info(s.toString());
			i++;
		}

	}
	
	/**
	 * Returns a list of all the objects of this index.
	 * @return a list of all the objects of this index.
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 * @throws IllegalIdException 
	 */
	public List<O> getAllObjects() throws IllegalIdException, IllegalAccessException, InstantiationException, OBException{
		List<O> db = new ArrayList<O>((int)databaseSize());
		int i = 0;
		long max = databaseSize();
		while (i < max) {
			O obj = getObject(i);
			db.add(obj);
			i++;
		}
		return db;
	}

	protected void initCache() throws OBException {
		super.initCache();
		bucketCache = new OBCacheByteArray<BC>(new BucketsLoader(),
				OBSearchProperties.getBucketsCacheSize());
	}

	private class BucketsLoader implements OBCacheHandlerByteArray<BC> {

		public long getDBSize() throws OBStorageException {
			return Buckets.size();
		}

		public BC loadObject(byte[] i) throws OBException,
				InstantiationException, IllegalAccessException,
				IllegalIdException {

			byte[] data = Buckets.getValue(i);
			if (data == null) {
				return null;
			}

			return instantiateBucketContainer(data, i);
		}

		@Override
		public void store(byte[] key, BC object) throws OBException {
			try {
				if (object.isModified()) {
					Buckets.put(key, object
							.serialize());
				}
			} catch (IOException e) {
				throw new OBException(e);
			}
		}

	}

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
	protected OperationStatus insertBucket(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {

		return insertBucketAux(b, object).insert(b, object);
	}

	/**
	 * Common functionality for insert operations
	 * 
	 * @param b
	 *            The bucket in which we will insert the object.
	 * @param object
	 *            The object to insert.
	 * @return
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected BC insertBucketAux(B b, O object) throws OBException,
			InstantiationException, IllegalAccessException {
		// get the bucket id.
		byte[] bucketId = getAddress(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.
		BC bc = getBucketContainer(bucketId);
		if (bc == null) {
			bc = instantiateBucketContainer(null, bucketId);
			this.bucketCache.put(bucketId, bc);
		}
		return bc;
	}

	/**
	 * Stores the given bucket b into the {@link #Buckets} storage device. The
	 * given bucket b should have been returned by {@link #getBucket(OB, int)}
	 * No checks are performed, we simply add the objects believing they are
	 * unique.
	 * 
	 * @param b
	 *            The bucket in which we will insert the object.
	 * @param object
	 *            The object to insert.
	 * @return A OperationStatus object with the new id of the object if the
	 *         object was inserted successfully.
	 * @throws OBStorageException
	 */
	@Override
	protected OperationStatus insertBucketBulk(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {

		return insertBucketAux(b, object).insertBulk(b, object);
	}

	protected byte[] convertLongToBytesAddress(long bucketId) {
		return fact.serializeLong(bucketId);
	}

	@Override
	public byte[] getAddress(B bucket) throws OBException {
		return convertLongToBytesAddress(getLongAddress(bucket));
	}

	protected abstract long getLongAddress(B b) throws OBException;

	/**
	 * Estimate ks for the given query object and the given list of objects.
	 * @param object
	 * @param objects
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected abstract void maxKEstimationAux(O object, List<O> objects)
	throws OBException, InstantiationException, IllegalAccessException ;

	/**
	 * Returns a k query for the given object.
	 * 
	 * @param object
	 *            (query object)
	 * @param k
	 *            the number of objects to accept in the query.
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws OBException
	 */
	protected abstract AbstractOBQuery<O> getKQuery(O object, int k)
			throws OBException, InstantiationException, IllegalAccessException;

	/**
	 * Search the f closest buckets to the given query. We drop the distance
	 * values for performance reasons, but we could add them if we wanted in the
	 * future.
	 * 
	 * @param query
	 *            the query to employ
	 * @param maxF
	 *            the max number of items that will be returned
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws OBException
	 */
	protected long[] searchBuckets(long query, int maxF)
			throws InstantiationException, IllegalAccessException, OBException {
		return sketchSet.searchBuckets(query, maxF, m);
	}

	/**
	 * Estimate the k needed for a k-nn query.
	 * 
	 * @param queryK
	 *            k of the k-nn query.
	 * @return Number of buckets that should be retrieved for this query.
	 * @throws OBException 
	 */
	public int estimateK(int queryK) throws OBException {
		int i = 0;
		for(int kval : this.userK){
			if(kval == queryK){
				break;
			}
			i++;
		}
		if(i == this.userK.length){
			throw new OBException("Wrong k value");
		}
		long x = Math.round(this.kEstimators[i].mean()
				+ (this.kEstimators[i].standardDeviation() * kAlpha));
		assert x <= Integer.MAX_VALUE;
		return (int) x;
	}

	public void freeze() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IllegalAccessException,
			InstantiationException, OBException {
		super.freeze();
		int i = 0;

		while (i < m) {
			int cx = 0;
			PivotResult r = super.selectPivots(HEIGHT, this.maskPivotSelector);
			while (cx < HEIGHT) {
				pivotGrid[i][cx] = getObject(r.getPivotIds()[cx]);
				cx++;
			}
			i++;
		}
		logger.info("Moving objects to the buckets...");
		freezeDefault();
		this.bucketCache.clearAll();
		// test the mask thing.
		logger.info("Loading masks...");
		loadMasks();
		logger.info("Calculating estimators...");
		calculateEstimators();
		logger.info("Index stats...");
		bucketStats();
		sketchStats();

	}

	private void sketchStats() throws OBStorageException {
		if (HEIGHT == 2) {
			StaticBin1D s = new StaticBin1D();
			int i = 0;
			while (i < distortionStats.length) {
				s.add(Math.abs(this.distortionStats[i][0]
						- this.distortionStats[i][1]));
				i++;
			}
			logger.info("Distortion:");
			logger.info(s.toString());
			logger.info("Distortion:" + s.mean() / databaseSize());
		}
	}

	protected void bucketStats() throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {

		logger.fine("Bucket stats starts!");
		CloseIterator<TupleBytes> it = Buckets.processAll();
		// assert Buckets.size() == A.size();
		StaticBin1D s = new StaticBin1D();

		while (it.hasNext()) {
			TupleBytes t = it.next();
			BC bc = instantiateBucketContainer(t.getValue(), t.getKey());
			s.add(bc.size());
		}
		getStats().putStats("BUCKET_STATS", s);
		logger.info("Bucket Stats:");
		logger.info(s.toString());
		it.closeCursor();
		logger.info("Spread: "
				+ (s.size() / Math.min(Math.pow(2, m), databaseSize())));
	}

	@Override
	public void close() throws OBException {
		bucketCache.clearAll();
		super.close();
	}

	public int[] getMaxK() {
		return userK;
	}

	public double getExpectedEP() {
		return expectedEP;
	}

}
