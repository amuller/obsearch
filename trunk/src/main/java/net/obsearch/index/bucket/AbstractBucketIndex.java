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
import net.obsearch.cache.OBCacheLoaderByteArray;
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
import net.obsearch.storage.TupleBytes;

public abstract class AbstractBucketIndex<O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractPivotOBIndex<O> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractBucketIndex.class);

	/**
	 * We store the buckets in this storage device.
	 */
	protected transient OBStore<TupleBytes> Buckets;

	/**
	 * Cache used for storing recently accessed Buckets.
	 */
	protected transient OBCacheByteArray<BC> bucketContainerCache;

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
		this.Buckets = fact.createOBStore("Buckets_byte_array", false, false);
		if (this.bucketContainerCache == null) {
			this.bucketContainerCache = new OBCacheByteArray<BC>(
					new BucketLoader());
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
		// get the bucket id.
		byte[] bucketId = getAddress(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.
		BC bc = this.bucketContainerCache.get(bucketId);
		if (bc == null) { // it was just created for the first
			// time.
			bc = instantiateBucketContainer(null, bucketId);
			bc.setPivots(getPivotCount());
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
		while (it.hasNext()) {
			TupleBytes t = it.next();
			BC bc = this.bucketContainerCache.get(t.getKey());
			s.add(bc.size());
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
		BC bc = this.bucketContainerCache.get(bucketId);
		if (bc == null) {
			res.setStatus(Status.NOT_EXISTS);
		} else {
			res = bc.delete(b, object);
			putBucket(bucketId, bc);
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
	
	public Statistics getStats() throws OBStorageException{
		Statistics s = super.getStats();
			s.putStats("Read Stats Buckets:", this.Buckets.getReadStats());
		return s;
	}

	/**
	 * @param bucket
	 */
	protected void putBucket(byte[] bucketId, BC bc) throws OBStorageException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		Buckets.put(bucketId, bc.getBytes());
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

	/**
	 * Class in charge of loading a Bucket Container from an stream of bytes.
	 */
	public class BucketLoader implements OBCacheLoaderByteArray<BC> {

		public long getDBSize() throws OBStorageException {
			return (int) A.size();
		}

		public BC loadObject(byte[] i) throws OutOfRangeException, OBException,
				InstantiationException, IllegalAccessException {
			ByteBuffer data = Buckets.getValue(i);
			if (data == null) {
				return null;
			} else {
				return instantiateBucketContainer(data, i);
			}

		}

	}

}