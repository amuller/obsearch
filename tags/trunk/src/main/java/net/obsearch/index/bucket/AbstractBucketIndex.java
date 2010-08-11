package net.obsearch.index.bucket;

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

/** 
 *  AbstractBucketIndex 
 *  
 *  @author      Arnoldo Jose Muller Molina    
 */

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;

import cern.colt.list.LongArrayList;
import net.obsearch.Index;
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
import net.obsearch.index.utils.StatsUtil;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;
import net.obsearch.storage.OBStorageConfig.IndexType;

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
	 * Useful iterator for buckets.
	 * @return
	 * @throws OBException 
	 * @throws OBStorageException 
	 */
	public Iterator<BC> iterateBuckets() throws OBStorageException, OBException{
		return new BucketIterator(Buckets.processAll());
	}
	
	
	protected class BucketIterator implements Iterator<BC>{
		
		private CloseIterator<TupleBytes> iter;
		
		public BucketIterator(CloseIterator<TupleBytes> iter){
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@Override
		public BC next() {
			TupleBytes t = iter.next();
			try{
				BC bc = instantiateBucketContainer(t.getValue(), t.getKey());
				if(! iter.hasNext()){ // close the cursor.
					iter.closeCursor();
				}
				return bc;
			}catch(Exception e){
				throw new IllegalArgumentException(e);
			}
			
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	/**
	 * Returns the bucket information for the given object.
	 * 
	 * @param object
	 *            The object that will be calculated
	 * @return The bucket information for the given object.
	 * @throws IllegalAccessException
	 */
	public abstract B getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException;

	/**
	 * Initializes the bucket byte array storage. To be called by the init
	 * procedure.
	 * 
	 * @throws OBException
	 */
	protected void initByteArrayBuckets() throws OBException {
		OBStorageConfig conf = new OBStorageConfig();
		conf.setTemp(false);
		conf.setDuplicates(true);
		conf.setBulkMode(!isFrozen());
		conf.setRecordSize(primitiveDataTypeSize());
		//conf.setIndexType(IndexType.HASH);
		this.Buckets = fact.createOBStore("Buckets_byte_array", conf);

	}

	/**
	 * Return the size in bytes of the underlying primitive datatype.
	 * 
	 * @return
	 */
	protected abstract int primitiveDataTypeSize();

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
		res = bc.insert(b, object);
		return res;
	}

	protected BC getBucketContainer(byte[] id) throws OBException, InstantiationException, IllegalAccessException {
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
		OperationStatus res;
		res = bc.insertBulk(b, object);
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

	protected void freezeDefault() throws  AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get n pivots.
		// ask the bucket for each object and insert those who are not excluded.
		// repeat iteratively with the objects that could not be inserted until
		// the remaining
		// objects are small enough.
		
		
		/*int i = 0;
		CloseIterator<TupleLong> it = A.processAll();
		while (it.hasNext()) {
			TupleLong t = it.next();
			long id = t.getKey();
			O o = super.bytesToObject(t.getValue());
			B b = getBucket(o);
			b.setId(id);
			this.insertBucketBulk(b, o);
			if(i % 100000 == 0){
				logger.info("Converting... " + i);
			}
			i++;
		}
		it.closeCursor();*/
		long i = 0;
		long max = databaseSize();
		logger.info("Database Size" + max);
		O o = type.newInstance();
		try{
		while(i < max){
			//O o = getObject(i);
			byte[] val = A.getValue(i);
			OBAsserts.chkAssert(val != null, "Object loaded from: " + i + " is null!");
			o.load(val);
			B b = getBucket(o);
			b.setId(i);
			this.insertBucketBulk(b, o);
			if(i % 100000 == 0){
				//logger.info("Converting... " + i + " b_size:\n" + stats.getStats("B_SIZE"));
				StaticBin1D s = stats.getStats("B_SIZE");
				if(s != null){
				logger.info("Converting... " + i + " " + s.mean() + " std: " + s.standardDeviation() );
				
				}else{
					logger.info("Converting... " + i );
				}
				//logger.info("Buckets " + Buckets.getStats() );
			}
			
			i++;
		}
		}catch(IOException e){
			throw new OBException(e);
		}
		
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
	 * @throws OBException 
	 */
	public abstract byte[] getAddress(B bucket) throws OBException;

	protected void bucketStats() throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException, OBException{

		logger.debug("Bucket stats");
		CloseIterator<TupleBytes> it = Buckets.processAll();
		// assert Buckets.size() == A.size();
		StaticBin1D s = new StaticBin1D();
		byte[] key = null;
		int counter = 0;
		int i = 0;
		while (it.hasNext()) {
			TupleBytes t = it.next();

			if (key != null && !Arrays.equals(t.getKey(), key)) {
				s.add(counter);
				key = t.getKey();
				counter = 1;
			} else {
				counter++;
				if (key == null) {
					// first time
					key = t.getKey();
				}
			}
			if (i % 10000 == 0) {
				logger.debug("Stats: " + i);
			}
			i++;
		} // add exlucion
		s.add(counter);
		assert A.size() == ((long) s.sum()) : "Size in stats: " + s.sum()
				+ " size in A: " + A.size();
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
	
	public OBStore<TupleBytes> getBuckets(){
		return Buckets;
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
	 * @throws IOException 
	 * @throws OBException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	protected abstract BC instantiateBucketContainer(byte[] data,
			byte[] address) throws InstantiationException, IllegalAccessException, OBException;

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