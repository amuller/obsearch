package net.obsearch.index.sorter;

import hep.aida.bin.StaticBin1D;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCacheByteArray;
import net.obsearch.cache.OBCacheHandlerByteArray;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.dimension.AbstractDimension;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.ghs.FixedPriorityQueue;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;
import net.obsearch.storage.OBStorageConfig.IndexType;

public abstract class AbstractBucketSorter<O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>, P extends Projection<P, CP>, CP>
		extends AbstractBucketIndex<O, B, Q, BC> {

	static final transient Logger logger = Logger
			.getLogger(AbstractBucketSorter.class.getName());

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
	private float kAlpha = 3f;
	/**
	 * K configuration that will be used by the user.
	 */
	protected int[] userK = new int[] { 1, 3, 10, 50 };
	/**
	 * The target EP value used in the estimation.
	 */
	private double expectedEP = 0.0;
	private Random r = new Random();

	/**
	 * We keep the projection list
	 */
	protected transient OBStoreLong projectionStorage;

	/**
	 * We keep here the projections.
	 */
	protected transient CP[] projections;

	/**
	 * Cache used for storing buckets
	 */
	protected transient OBCacheByteArray<BC> bucketCache;

	/**
	 * Pivot count for each bucket.
	 */
	protected int bucketPivotCount;

	public AbstractBucketSorter(Class type,
			IncrementalPivotSelector pivotSelector, int pivotCount,
			int bucketPivotCount) throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
		this.bucketPivotCount = bucketPivotCount;
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

	@Override
	public byte[] getAddress(B bucket) throws OBException {
		return getProjection(bucket).getAddress();
	}

	protected abstract P getProjection(B b) throws OBException;

	protected abstract byte[] compactRepresentationToBytes(CP cp);

	protected abstract CP bytesToCompactRepresentation(byte[] data);

	protected abstract Class<CP> getPInstance();

	/**
	 * Load the masks from the storage device into memory.
	 * 
	 * @throws OBException
	 */
	protected void loadMasks() throws OBException {
		if (projections != null) {
			return;
		}
		logger.info("Loading masks!");
		OBAsserts.chkAssert(projectionStorage.size() <= Integer.MAX_VALUE,
				"Capacity exceeded");
		projections = (CP[]) Array.newInstance(getPInstance(),
				(int) projectionStorage.size());
		CloseIterator<TupleLong> it = projectionStorage.processAll();

		int i = 0;
		while (it.hasNext()) {
			TupleLong t = it.next();
			CP cp = this.bytesToCompactRepresentation(t.getValue());
			projections[i] = cp;
			i++;
		}
		logger.info("Loaded: " + i + " masks");

		it.closeCursor();
	}

	/**
	 * Calculates the distance between a query and some projection
	 * 
	 * @param query
	 * @return
	 */
	protected abstract P calculateDistance(P query, CP proj);

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
	protected List<P> searchBuckets(P query, int maxF)
			throws InstantiationException, IllegalAccessException, OBException {
		loadMasks();
		FixedPriorityQueue<P> queue = new FixedPriorityQueue<P>(maxF);
		for (CP p : this.projections) {
			P result = calculateDistance(query, p);
			queue.add(result);
		}
		return queue.getSortedData();
	}

	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, InstantiationException, IllegalAccessException {
		super.init(fact);
		OBStorageConfig conf = new OBStorageConfig();
		conf.setTemp(false);
		conf.setDuplicates(false);
		conf.setIndexType(IndexType.FIXED_RECORD);
		conf.setRecordSize(getCPSize());
		this.projectionStorage = fact.createOBStoreLong("projections", conf);
	}

	protected BC getBucketContainer(byte[] id) throws OBException,
			InstantiationException, IllegalAccessException {
		// BC bc = instantiateBucketContainer(null, id);
		BC container = this.bucketCache.get(id);
		return container;

	}

	protected void initByteArrayBuckets() throws OBException {
		OBStorageConfig conf = new OBStorageConfig();
		conf.setTemp(false);
		conf.setDuplicates(false);
		conf.setBulkMode(!isFrozen());
		this.Buckets = fact.createOBStore("Buckets_byte_array", conf);

	}

	/**
	 * Return the compact representation size
	 * 
	 * @return
	 */
	protected abstract int getCPSize();

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

		long[] sample = AbstractDimension.select(sampleSize, r, null,
				(Index) this, null);
		O[] sampleSet = getObjects(sample);
		i = 0;
		for (O o : sampleSet) {
			logger.info("Estimating k sample #: " + i + " of " + sampleSize);
			maxKEstimationAux(o);
			i++;
		}

		i = 0;
		for (StaticBin1D s : kEstimators) {
			logger.info(" k" + userK[i]);
			logger.info(s.toString());
			i++;
		}

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

			/*
			 * if (object.isModified()) { OperationStatus s =
			 * Buckets.putIfNew(key, object .serialize()); if(s.)
			 * stats.addExtraStats("B_SIZE", object.size()); }
			 */

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

		byte[] bucketId = getAddress(b);

		BC bc = instantiateBucketContainer(null, bucketId);
		OperationStatus s = bc.insert(b, object);
		// store the data in the index.

		// we have to re-do everything.
		byte[] bucketData = Buckets.getValue(bucketId);
		bc = instantiateBucketContainer(bucketData, bucketId);
		s = bc.insert(b, object);
		if (s.getStatus() == Status.OK) {
			projections = null; // make the sketch set void
			projectionStorage.put(b.getId(), bucketId);
		}
		Buckets.put(bucketId, bc.serialize());

		this.bucketCache.put(bucketId, bc);
		stats.addExtraStats("B_SIZE", bc.size());
		return s;
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

		projections = null; // make the sketch set void

		byte[] bucketId = getAddress(b);
		BC bc = instantiateBucketContainer(null, bucketId);
		OperationStatus s = bc.insertBulk(b, object);

		// we have to re-do everything.
		byte[] bucketData = Buckets.getValue(bucketId);
		bc = instantiateBucketContainer(bucketData, bucketId);
		s = bc.insertBulk(b, object);
		// long prevSize = Buckets.size();
		Buckets.put(bucketId, bc.serialize());
		/*
		 * if(bucketData == null){ assert Buckets.size() == (prevSize + 1); }
		 */
		projectionStorage.put(b.getId(), bucketId);
		this.bucketCache.put(bucketId, bc);
		stats.addExtraStats("B_SIZE", bc.size());
		return s;
	}

	@Override
	public void close() throws OBException {
		bucketCache.clearAll();
		projectionStorage.close();
		super.close();
	}

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
	 * Returns a list of all the objects of this index.
	 * 
	 * @return a list of all the objects of this index.
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalIdException
	 */
	public List<O> getAllObjects() throws IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {
		List<O> db = new ArrayList<O>((int) databaseSize());
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

	/**
	 * Estimate ks for the given query object and the given list of objects.
	 * 
	 * @param object
	 * @param objects
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected abstract void maxKEstimationAux(O object) throws OBException,
			InstantiationException, IllegalAccessException;

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
		for (int kval : this.userK) {
			if (kval == queryK) {
				break;
			}
			i++;
		}
		if (i == this.userK.length) {
			throw new OBException("Wrong k value");
		}
		long x = Math.round(this.kEstimators[i].mean()
				+ (this.kEstimators[i].standardDeviation() * kAlpha));
		assert x <= Integer.MAX_VALUE;
		return (int) x;
	}

	public int[] getMaxK() {
		return userK;
	}

	public double getExpectedEP() {
		return expectedEP;
	}

	public int getBucketPivotCount() {
		return bucketPivotCount;
	}

	public void bucketStats() throws OBStorageException, IllegalIdException,
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
		
	}

}