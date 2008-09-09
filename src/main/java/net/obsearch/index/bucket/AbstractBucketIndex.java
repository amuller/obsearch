package net.obsearch.index.bucket;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.log4j.Logger;

import cern.colt.list.LongArrayList;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCacheByteArray;
import net.obsearch.cache.OBCacheHandlerByteArray;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.pivot.AbstractPivotOBIndex;
import net.obsearch.index.rosa.AbstractRosaFilter;
import net.obsearch.index.utils.StatsUtil;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;

public abstract class AbstractBucketIndex<O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractPivotOBIndex<O> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractBucketIndex.class);

	/**
	 * We store the buckets in this storage device.
	 */
	protected transient OBStore<TupleBytes> Buckets;

	public AbstractBucketIndex(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
	}

	/**
	 * If elementSource == null returns id, otherwise it returns
	 * elementSource[id]
	 * 
	 * @return
	 */
	protected long idMap(long id, LongArrayList elementSource)
			throws OBException {
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
	 * @throws IllegalAccessException
	 */
	protected abstract B getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException;

	/**
	 * Initializes the bucket byte array storage. To be called by the init
	 * procedure.
	 * 
	 * @throws OBException
	 */
	protected void initByteArrayBuckets() throws OBException {
		this.Buckets = fact.createOBStore("Buckets_byte_array", false, true, ! isFrozen());

	}

	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, InstantiationException, IllegalAccessException {
		super.init(fact);
		initByteArrayBuckets();
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
		// get the bucket id.
		byte[] bucketId = getAddress(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.

		BC bc = getBucketContainer(bucketId);
		OperationStatus res = new OperationStatus();
		synchronized (bc) {
			res = bc.insert(b, object);
		}
		return res;
	}
	
	

	private BC getBucketContainer(byte[] id) {
		BC bc = instantiateBucketContainer(null, id);
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
	protected OperationStatus insertBucketBulk(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get the bucket id.
		byte[] bucketId = getAddress(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.
		BC bc = getBucketContainer(bucketId);

		OperationStatus res = new OperationStatus();
		synchronized (bc) {
			res = bc.insertBulk(b, object);
		}
		return res;
	}

	public OperationStatus exists(O object) throws OBException,
			IllegalAccessException, InstantiationException {
		OperationStatus res = new OperationStatus();
		res.setStatus(Status.NOT_EXISTS);
		B b = getBucket(object);
		byte[] bucketId = getAddress(b);
		BC bc = getBucketContainer(bucketId);
		res = bc.exists(b, object);
		return res;
	}

	@Override
	public void close() throws OBException {
		if (Buckets != null) {
			Buckets.close();
		}
		super.close();
	}

	/**
	 * Gets the byte address of the given object
	 * 
	 * @param object
	 *            An object.
	 * @return The rosa filter value of the given object.
	 */
	protected abstract byte[] getAddress(B bucket);

	protected void bucketStats() throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {

		CloseIterator<TupleBytes> it = Buckets.processAll();
		StaticBin1D s = new StaticBin1D();
		byte[] key = null;
		int counter = 0;
		while (it.hasNext()) {
			TupleBytes t = it.next();
			if(key != null && ! Arrays.equals(t.getKey(), key)){
				s.add(counter);
			}else{
				counter++;
			}
		} // add exlucion
		logger.info(StatsUtil.prettyPrintStats("Bucket distribution", s));
		it.closeCursor();
	}

	public OperationStatus deleteAux(O object) throws OBException,
			IllegalAccessException, InstantiationException {

		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
		B b = getBucket(object);
		byte[] bucketId = getAddress(b);
		BC bc = getBucketContainer(bucketId);
		if (bc == null) {
			res.setStatus(Status.NOT_EXISTS);
		} else {
			res = bc.delete(b, object);

		}

		return res;
	}

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

	@Override
	public OperationStatus insertAuxBulk(long id, O object) throws OBException,
			IllegalAccessException, InstantiationException {

		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
		B b = getBucket(object);
		b.setId(id);
		res = this.insertBucketBulk(b, object);
		res.setId(id);
		return res;
		
	}

	/**
	 * Get a bucket container from the given data.
	 * 
	 * @param data
	 *            The data from which the bucket container will be loaded.
	 * @return A new bucket container ready to be used.
	 */
	protected abstract BC instantiateBucketContainer(ByteBuffer data,
			byte[] address);

	/**
	 * Print debug info. of the given object.
	 */
	public String debug(O object) throws OBException, InstantiationException,
			IllegalAccessException {
		B b = this.getBucket(object);
		return b.toString() + "\naddr:\n " + Arrays.toString(getAddress(b))
				+ "\n";
	}

}