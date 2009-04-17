package net.obsearch.index.bucket.sleek;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.junit.Test;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.filter.Filter;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.impl.BucketObjectShort;
import net.obsearch.ob.OBShort;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.query.OBQueryShort;
import net.obsearch.stats.Statistics;
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
 * AbstractSleekBucket is designed to hold objects that belong to one bucket.
 * The bucket will hold ALL the buckets. Optionally, objects inside the bucket
 * can be used as pivotCount. Format: <object count> <pivot 1> <pivot 2> ...
 * <pivot n> <distance to pivot 1> <distance to pivot 2> ... <distance to pivot
 * n> <obj2> .... This bucket design focuses on pivotCount local to a partition
 * instead of global pivotCount. Also, the storage and reading of the container
 * is responsability of the index. The class does not access directly the index
 * unlike the bucket.* classes we now have something that partitions the data
 * properly.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public class SleekBucketShort<O extends OBShort> implements
		BucketContainer<O, BucketObjectShort<O>, OBQueryShort<O>> {

	/**
	 * Number of objects in the bucket.
	 */
	private int count = 0;
	/**
	 * Number of pivotCount in the bucket.
	 */
	private int pivotCount;

	/**
	 * The list of pivot objects;
	 */
	private List<BucketObjectShort<O>> pivots;

	/**
	 * The list of objects.
	 */
	private List<BucketObjectShort<O>> objects;

	/**
	 * Type of the object that is being stored.
	 */
	private Class<O> type;
	
	/**
	 * Flag used to know when the data has been modified.
	 */
	private boolean modified = false;

	/**
	 * Positive means that records are fixed. Negative means the maximum size of
	 * the records in this bucket. Zero means that no objects have been added.
	 * Mode is only updated before writing a bucket.
	 */
	private int mode = 0;

	/**
	 * Create an empty, new bucket.
	 * 
	 * @param pivotCount
	 *            number of pivotCount to use.
	 */
	public SleekBucketShort(Class<O> type, int pivots) {
		this.pivotCount = pivots;
		this.type = type;
		this.pivots = new ArrayList<BucketObjectShort<O>>(pivotCount);
		objects = new ArrayList<BucketObjectShort<O>>(count);
	}

	/**
	 * Create a new SleekBucket based on the given data.
	 * 
	 * @param pivotCount
	 * @param data
	 * @throws IOException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws IOException 
	 */
	public SleekBucketShort(Class<O> type, int pivots, byte[] data)
			throws InstantiationException, IllegalAccessException, OBException, IOException
			 {
		this(type, pivots);
		parseData(pivots, data);
	}

	/**
	 * Parses the data from the given byte array.
	 * 
	 * @param data
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws IOException
	 * @throws OBException
	 */
	public void parseData(int pivotCount, byte[] data)
			throws InstantiationException, IllegalAccessException, OBException,
			IOException {
		pivots = new ArrayList<BucketObjectShort<O>>(pivotCount);
		ByteBuffer buf = ByteConversion.createByteBuffer(data);
		// read the number of objects included in the bucket.
		count = buf.getInt();
		mode = buf.getInt();
		objects = new ArrayList<BucketObjectShort<O>>(count);
		// get the pivots!
		int i = 0;
		while (i < Math.min(pivotCount , count)) {			
			pivots.add(readBucketObject(0, buf));
			i++;
		}
		// now we can start getting objects.
		// continue loading objects
		while (i < count) {			
			objects.add(readBucketObject(pivotCount, buf));
			i++;
		}
		assert count == (pivots.size() + objects.size());	
		assert (! buf.hasRemaining()) : "Remaining: " + buf.remaining() + " Position: " + buf.position() + " capacity: " + buf.capacity() + " count: " + count;
		
	}
	
	/**
	 * Read the next object in the given buffer
	 * @param pivots number of pivots to use
	 * @return A new BucketObject
	 * @throws IOException 
	 * @throws OBException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	private BucketObjectShort<O> readBucketObject(int pivots, ByteBuffer buf) throws OBException, IOException, InstantiationException, IllegalAccessException{
		O obj = type.newInstance();
		BucketObjectShort<O> bucket = new BucketObjectShort<O>();
		bucket.read(buf, pivots);
		byte[] objectRawData = getNextObjectChunk(buf);
		obj.load(objectRawData);
		bucket.setObject(obj);
		return bucket;
	}

	private ByteConstants getAppropiate(int size) {
		if (size <= Byte.MAX_VALUE) {
			return ByteConstants.Byte;
		} else if (size <= Short.MAX_VALUE) {
			return ByteConstants.Short;
		} else if (size <= Integer.MAX_VALUE) {
			return ByteConstants.Int;
		} else {
			return null;
		}
	}

	public boolean equals(Object o) {
		SleekBucketShort<O> another = (SleekBucketShort<O>) o;
		if(another.count != this.count){
			return false;
		}
		try {
			for (BucketObjectShort<O> p : pivots) {
				if (! another.pivots.contains(p)) {
					return false;
				}
			}
			
			for(BucketObjectShort<O> b : objects){
				if (! another.objects.contains(b)) {
					return false;
				}
			}
			
			return true;

		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	private byte[] getNextObjectChunk(ByteBuffer buf) {

		byte[] res;
		int size;
		if (mode > 0) {
			size = mode;
		} else {
			assert mode != 0;
			ByteConstants mo = getAppropiate(Math.abs(mode));
			if (mo == ByteConstants.Byte) {
				size = buf.get();
			} else if (mo == ByteConstants.Short) {
				size = buf.getShort();
			} else if (mo == ByteConstants.Int) {
				size = buf.getInt();
			} else {
				assert false;
				return null;
			}

		}
		res = new byte[size];
		buf.get(res);
		return res;
	}

	private void putNextObjectChunk(ByteBuffer buf, byte[] data) {

		int size = data.length;
		if (mode > 0) {
			assert data.length == mode;
		} else {
			assert mode != 0;
			ByteConstants mo = getAppropiate(Math.abs(mode));
			if (mo == ByteConstants.Byte) {
				assert size <= Byte.MAX_VALUE;
				buf.put((byte) size);				
			} else if (mo == ByteConstants.Short) {
				assert size <= Short.MAX_VALUE;
				buf.putShort((short) size);
			} else if (mo == ByteConstants.Int) {
				assert size <= Integer.MAX_VALUE;
				buf.putInt(size);
			} else {
				assert false;
			}

		}
		buf.put(data);

	}

	private void removeAll() {
		pivots.clear();
		objects.clear();
		count = 0;
	}
	
	/**
	 * Remove the object if it is found in the list
	 * @param list
	 * @param object
	 * @return the object removed or null otherwise
	 */
	private BucketObjectShort<O> removeObject(List<BucketObjectShort<O>> list, O object){
		ListIterator<BucketObjectShort<O>> it = list.listIterator();
		while (it.hasNext()) {
			BucketObjectShort<O> b = it.next();
			if (b.getObject().equals(object)) {
				it.remove();
				return b;
			}
		}
		return null;
	}

	@Override
	public OperationStatus delete(BucketObjectShort<O> bucket, O object)
			throws OBException, IllegalIdException, IllegalAccessException,
			InstantiationException {
		OperationStatus result = new OperationStatus();
		result.setStatus(Status.NOT_EXISTS);
		// if we have to remove a pivot we have to remove all objects,
		// shuffle them and re-insert them again.
		// start with the pivots.
		BucketObjectShort<O> removed = removeObject(pivots, object);
		if (removed != null) {
			
			// removed one of the pivots, we have to re-build the container.
			result.setStatus(Status.OK);
			result.setId(removed.getId());
			assert this.existsObjects(object, pivots) == null;
			List<BucketObjectShort<O>> objs = new ArrayList<BucketObjectShort<O>>(count);
			for (BucketObjectShort<O> p : pivots) {
				objs.add(p);
			}
			for (BucketObjectShort<O> b : objects) {
				objs.add(b);
			}

			removeAll(); // empty the bucket.
			for (BucketObjectShort<O> o : objs) {
				insertBulk(o, o.getObject());
			}
		} else {
			// bucket.
			removed = removeObject(objects, object);
			if(removed != null){
				result.setStatus(Status.OK);
				result.setId(removed.getId());
			}
		}
		if(result.getStatus() == Status.OK){
			this.modified = true;
		}
		count = pivots.size() + objects.size();
		assert count == (pivots.size() + objects.size());
		return result;
	}
	
	public boolean isModified(){
		return modified;
	}

	@Override
	public OperationStatus exists(BucketObjectShort<O> bucket, O object)
			throws OBException, IllegalIdException, IllegalAccessException,
			InstantiationException {
		OperationStatus res = new OperationStatus();
		BucketObjectShort<O> inPivots = existsObjects(object, pivots);
		BucketObjectShort<O> inObjects = existsObjects(object, objects);
		if (inPivots != null || inObjects != null) {
			res.setStatus(Status.EXISTS);
			if(inPivots != null){
				res.setId(inPivots.getId());
			}else if(inObjects != null){
				res.setId(inObjects.getId());
			}
		} else {
			res.setStatus(Status.NOT_EXISTS);
		}
		return res;
	}
	/**
	 * Check if the given object exists on the given list
	 * @param object the object to search
	 * @return the bucket if it exists otherwise null
	 */
	private BucketObjectShort<O> existsObjects(O object, List<BucketObjectShort<O>> list){
		for(BucketObjectShort<O> o : list){
			if( o.getObject().equals(object)){
				return o;
			}
		}
		return null;
	}

	@Override
	public int getPivots() {
		return pivotCount;
	}

	@Override
	public OperationStatus insert(BucketObjectShort<O> bucket, O object)
			throws OBException, IllegalIdException, IllegalAccessException,
			InstantiationException {
		OperationStatus exists = exists(bucket, object);
		if (exists.getStatus() == Status.EXISTS) {
			return exists;
		} else {
			return insertBulk(bucket, object);
		}

	}

	@Override
	public OperationStatus insertBulk(BucketObjectShort<O> bucket, O object)
			throws OBException, IllegalIdException, IllegalAccessException,
			InstantiationException {
		OperationStatus res = new OperationStatus();
		if (pivots.size() < pivotCount) { // not enough pivots? we add it to the
			// pivots.
			//OBAsserts.chkAssert(bucket.getObject().equals(object), "Both objects should be the same");
			bucket.setObject(object);
			
			pivots.add(bucket);
			res.setStatus(Status.OK);
		} else {
			// enough pivots, we calculate the pivot vector
			objects.add(createBucket(bucket, object));
			res.setStatus(Status.OK);
		}
		modified = true;
		if(res.getStatus() == Status.OK){
			// inserted, update id
			res.setId(bucket.getId());
		}
		count++;
		assert count == pivots.size() + objects.size();
		return res;
	}

	private BucketObjectShort<O> createBucket(BucketObjectShort<O> bucket, O object) throws OBException {
		short[] pivotVector = new short[pivotCount];
		int i = 0;
		while (i < pivotCount) {
			pivotVector[i] = object.distance(pivots.get(i).getObject());
			i++;
		}
		bucket.setSmapVector(pivotVector);
		return bucket;
	}

	@Override
	public void search(AbstractOBQuery<O> q, BucketObjectShort<O> bucket,
			Filter<O> filter, Statistics stats) throws IllegalAccessException,
			OBException, InstantiationException, IllegalIdException {
		search((OBQueryShort) q, bucket, filter, stats);
	}

	@Override
	public void search(OBQueryShort<O> query, BucketObjectShort<O> bucket,
			ByteBuffer b, Filter<O> filter, Statistics stats)
			throws IllegalAccessException, OBException, InstantiationException,
			IllegalIdException {
		throw new IllegalArgumentException();
	}

	@Override
	public void search(OBQueryShort<O> query, BucketObjectShort<O> bucket,
			Filter<O> filter, Statistics stats) throws IllegalAccessException,
			OBException, InstantiationException, IllegalIdException {
		// must add also the pivots.
		
		int i = 0;
		short[] pivotVector = new short[pivotCount];
		while (i < pivotCount) {
			BucketObjectShort<O> p = pivots.get(i); // get the pivot.
			short distance = query.getObject().distance(p.getObject());
			query.add(p.getId(), p.getObject(), distance);
			pivotVector[i] = distance;
			stats.incDistanceCount();
			i++;
		}
		this.createBucket(bucket, query.getObject());		
		BucketObjectShort<O> b = new BucketObjectShort<O>(pivotVector, -1,
				query.getObject());
		// now we can match the remaining of the objects.
		for (BucketObjectShort<O> db : objects) {
			short lowerBound = b.lInf(db);
			stats.incSmapCount();
			if (query.isCandidate(lowerBound) && (filter == null || filter.accept(db.getObject(), query.getObject()))) {
				short distance = query.getObject().distance(db.getObject());
				stats.incDistanceCount();
				query.add(-1, db.getObject(), distance);
			}
		}
	}

	private int objectsCount(){
		return objects.size();
	}
	
	private int pivotsCount(){
		return pivots.size();
	}
	
	/**
	 * Serialize the bucket into a stream of bytes.
	 * 
	 * @return The list of
	 * @throws IOException
	 * @throws OBException
	 */
	public byte[] serialize() throws OBException {
		//OBAsserts.chkAssert(size() > 0, "Do not serialize an empty bucket");
		
		ArrayList<byte[]> serializedPivots = new ArrayList<byte[]>(pivotsCount());
		ArrayList<byte[]> serializedObjects = new ArrayList<byte[]>(objectsCount());
		int objectBytes = 0;
		// serialize pivots
		int minSize = Integer.MAX_VALUE;
		int maxSize = Integer.MIN_VALUE;
		try{
		for (BucketObjectShort<O> p : pivots) {
			byte[] t = p.getObject().store();
			objectBytes += t.length;
			serializedPivots.add(t);
			minSize = Math.min(t.length, minSize);
			maxSize = Math.max(t.length, maxSize);
		}
		// serialize all other objects.
		for (BucketObjectShort<O> p : objects) {
			byte[] t = p.getObject().store();
			objectBytes += t.length;
			serializedObjects.add(t);
			minSize = Math.min(t.length, minSize);
			maxSize = Math.max(t.length, maxSize);
		}
		}catch(IOException e){
			throw new OBException(e);
		}
		// calculate the mode of the index.
		if (minSize == maxSize) {
			mode = maxSize;
		} else {
			mode = -1 * maxSize;
		}
		int miniHeaders;
		if (mode > 0) {
			miniHeaders = 0; // no space required.
		} else {
			miniHeaders = getAppropiate(Math.abs(mode)).getSize()
					* size();
		}
		int bufferSize = HEADER_SIZE + objectBytes
				+ (objectsCount() * pivotCount * DISTANCE_SIZE)
				+ miniHeaders + (Index.ID_SIZE * size());
		ByteBuffer buf = ByteConversion.createByteBuffer(bufferSize);
		buf.putInt(size()); // write size
		buf.putInt(mode);

		// write the pivots:

		
		
		int i = 0;
		while (i < serializedPivots.size()) {
			BucketObjectShort<O> b = pivots.get(i);
			putObject(b, serializedPivots.get(i), buf);
			i++;
		}

		// write the rest of the objects.
		i = 0;
		while (i < serializedObjects.size()) {
			BucketObjectShort<O> b = objects.get(i);
			putObject(b, serializedObjects.get(i), buf);
			i++;
		}
		assert count == pivots.size() + objects.size();
		assert buf.remaining() == 0 : "Remaining: " + buf.remaining();
		return buf.array();
	}
	
	/**
	 * Write the given bucket and object data into buf
	 * @param b bucket
	 * @param object object data
	 * @param buf the buffer in which we will write everything
	 */
	private void putObject(BucketObjectShort<O> b, byte[] object, ByteBuffer buf){
		// write the bucket
		b.write(buf);
		// now we can write the object
		putNextObjectChunk(buf, object);
	}

	/**
	 * Header size
	 */
	final int HEADER_SIZE = ByteConstants.Int.getSize() * 2;
	final int DISTANCE_SIZE = ByteConstants.Short.getSize();

	@Override
	public void setKey(byte[] key) {
		throw new IllegalArgumentException();
	}

	@Override
	public void setPivots(int pivots) {
		this.pivotCount = pivots;
	}

	@Override
	public int size()  {
		return count;
	}
	
	public String toString(){
		return "Size: " + size() + " pivots: " + pivotCount + " mode: " + mode;
	}

}
