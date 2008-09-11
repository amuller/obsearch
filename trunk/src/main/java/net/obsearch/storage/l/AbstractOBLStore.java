package net.obsearch.storage.l;

import hep.aida.bin.StaticBin1D;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCacheHandler;
import net.obsearch.cache.OBCacheHandlerByteArray;
import net.obsearch.cache.OBCacheHandlerLong;
import net.obsearch.cache.OBCacheLong;
import net.obsearch.constants.ByteConstants;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.Tuple;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;

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
 * AbstractOBLStorage holds very large buckets. It allows efficient insertion
 * deletion. Search is performed sequentially over each record. This is a
 * meta-storage. It works on top of another storage.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public abstract class AbstractOBLStore<T extends Tuple> implements OBStore<T> {

	private static final transient Logger logger = Logger
	.getLogger(AbstractOBLStore.class);
	/**
	 * Stores the handles of the files.
	 */
	private OBCacheLong<RandomAccessFileHolder> handles;

	private OBStore<TupleBytes> storage;

	private boolean duplicates;

	private String name;

	protected OBStoreFactory fact;

	private File baseFolder;

	/**
	 * Used to simplify the logic of the implementation. It has the added
	 * benefit that there is no fragmentation in the bucket. The record size has
	 * effect only if duplicates == true.
	 */
	private int recordSize;

	protected AbstractOBLStore(String name, OBStore<TupleBytes> storage,
			OBStoreFactory fact, boolean duplicates, int recordSize,
			File baseFolder) throws OBException {
		this.duplicates = duplicates;
		this.name = name;
		this.fact = fact;
		this.storage = storage;
		this.recordSize = recordSize;
		this.baseFolder = new File(baseFolder, name);
		this.handles = new OBCacheLong<RandomAccessFileHolder>(new HandlerLoader(),
				OBSearchProperties.getHandlesCacheSize());
	}

	@Override
	public void close() throws OBStorageException {
		try {
			handles.clearAll();
		} catch (Exception e) {
			throw new OBStorageException(e);
		}
		storage.close();
	}

	/**
	 * Create a new file based on the given id.
	 * 
	 * @param id
	 *            Id of the file
	 * @return return the file for the given id.
	 */
	private File generateBucketFile(int id) {
		StringBuilder fileName = new StringBuilder();
		for (char c : Integer.toHexString(id).toCharArray()) {
			fileName.append(File.separatorChar);
			fileName.append(c);

		}
		fileName.append(".d");
		return new File(baseFolder, fileName.toString());
	}

	/**
	 * Deletes the given bucket address.
	 * 
	 * @param f
	 */
	private void deleteBucket(File f) throws OBException {
		OBAsserts.chkAssert(f.delete(), "Could not delete file: "
				+ f.toString());
	}

	@Override
	public OperationStatus delete(byte[] key) throws OBStorageException {
		try {
			if (duplicates) {
				ByteBuffer bucket = storage.getValue(key);
				if (bucket != null) {
					int id = bucket.getInt();
					handles.remove(id);
					deleteBucket(generateBucketFile(id));
				}
			}
			return storage.delete(key);

		} catch (Exception e) {
			throw new OBStorageException(e);
		}
	}

	@Override
	public void deleteAll() throws OBStorageException {
		try {
			CloseIterator<TupleBytes> it = storage.processAll();
			while (it.hasNext()) {
				TupleBytes b = it.next();
				delete(b.getKey());
			}
		} catch (Exception e) {
			throw new OBStorageException(e);
		}
	}

	@Override
	public OBStoreFactory getFactory() {
		return fact;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public StaticBin1D getReadStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteBuffer getValue(byte[] key) throws IllegalArgumentException,
			OBStorageException {
		if (duplicates) {
			throw new IllegalArgumentException();
		}
		return storage.getValue(key);
	}

	@Override
	public long nextId() throws OBStorageException {
		return storage.nextId();
	}

	@Override
	public CloseIterator<TupleBytes> processRange(byte[] low, byte[] high)
			throws OBStorageException {
		return new ByteArrayIterator(low, high, false, false);
	}

	@Override
	public CloseIterator<TupleBytes> processRangeReverse(byte[] low, byte[] high)
			throws OBStorageException {
		return new ByteArrayIterator(low, high, false, true);
	}

	@Override
	public OperationStatus put(byte[] key, ByteBuffer value)
			throws OBStorageException {
		if (!duplicates) {
			return storage.put(key, value);
		}
		try {
			OBAsserts.chkAssertStorage(value.array().length == recordSize,
					"Invalid record size");

			OperationStatus res = new OperationStatus();
			res.setStatus(Status.OK);
			// add the value at the end of the file.
			int id = -1;
			ByteBuffer data = storage.getValue(key);
			if (data == null) {
				long idl = storage.nextId();
				OBAsserts.chkAssert(idl <= Integer.MAX_VALUE,
						"Exceeded possible number of buckets, fatal error");
				id = (int) idl;
				ByteBuffer j = ByteConversion
						.createByteBuffer(ByteConstants.Int.getSize());
				j.putInt(id);
				storage.put(key, j);
			} else {
				id = data.getInt();
			}
			RandomAccessFile bucket = handles.get(id).getFile();
			// record last position
			long lastPosition = bucket.length();
			// extend the file
			bucket.setLength(bucket.length() + recordSize);
			// write the data
			bucket.seek(lastPosition);
			bucket.write(value.array());
			return res;
		} catch (Exception e) {
			throw new OBStorageException(e);
		}
	}

	@Override
	public void setReadStats(StaticBin1D stats) {
		// TODO Auto-generated method stub

	}

	@Override
	public long size() throws OBStorageException {
		// inneficient but simple.
		CloseIterator<TupleBytes> it = storage.processAll();
		long res = 0;
		try {
			while (it.hasNext()) {
				TupleBytes t = it.next();
				int id = t.getValue().getInt();
				RandomAccessFile currentBucket = handles.get(id).getFile();
				res += currentBucket.length() / recordSize;
			}
			it.closeCursor();
			return res;
		} catch (Exception e) {
			throw new OBStorageException(e);
		}

	}

	protected abstract class CursorIterator<T> implements CloseIterator<T> {
		private CloseIterator<TupleBytes> it;
		// TODO: If the cache closes the object, then
		// the iterator will become invalid. We have to
		// check for such case.
		private FileHolder currentBucket;
		private byte[] currentData = new byte[recordSize];
		private TupleBytes currentTuple;
		private long previousIndex = 0;
		/**
		 * If this iterator goes backwards.
		 */
		private boolean backwardsMode;

		byte[] min;
		byte[] max;

		protected CursorIterator(byte[] min, byte[] max, boolean full,
				boolean backwards) throws OBStorageException {
			if (full) {
				it = storage.processAll();
			} else if (backwards) {
				it = storage.processRangeReverse(min, max);
			} else {
				it = storage.processRange(min, max);
			}
			this.min = min;
			this.max = max;
		}

		private boolean isCurrentFileFinished(){
			boolean res;
			try {
				long p = -1;
				long l = -1;
				// first time || after the first time
				if (currentBucket != null) {
					p = currentBucket.getFilePointer();
					l = currentBucket.length();
					
				}
				
				res = currentBucket == null || p == l;
				//logger.debug("p: " + p + " l: " + l + " finished: " + res + "it.hasnext " + it.hasNext() + " len: " );
			} catch (Exception e) {
				throw new NoSuchElementException(e.toString());
			}
			return res;
		}

		private void loadNext() throws OBException, InstantiationException,
				IllegalAccessException, IOException {
			if (isCurrentFileFinished()) {
				if (it.hasNext()) {
					TupleBytes tuple = it.next();
					assert currentTuple == null
							|| !Arrays.equals(tuple.getKey(), currentTuple
									.getKey());
					currentTuple = tuple;
					int id = tuple.getValue().getInt();
					
					this.currentBucket = new FileHolder(handles.get(id), id);
					
					currentBucket.seek(0); // reset the file pointer.
					
					assert !isCurrentFileFinished() : "Empty buckets are wrong";
					previousIndex = 0;
				} else {
					it = null; // we are done.
					
					
				}
			}
			// we now just have to read the bytes
			// read the size of the bytes stored.
			previousIndex = currentBucket.getFilePointer();
			// currentData = new byte[recordSize];
			assert recordSize == currentData.length;
			currentBucket.readFully(currentData);
		}

		@Override
		public boolean hasNext() {
			boolean res = it != null && (it.hasNext() || !isCurrentFileFinished());
			//logger.debug("last hasnext: " + res + " it.hasnext" + it.hasNext());
			return res;
		}

		@Override
		public T next() {
			try {
				loadNext();
				return createTuple(currentTuple.getKey(), ByteConversion
						.createByteBuffer(currentData));
			} catch (Exception e) {
				e.printStackTrace();
				throw new UnsupportedOperationException(e);
			}

		}

		@Override
		public void remove() {
			try {

				if (!Arrays.equals(min, max)) {
					throw new NoSuchElementException(
							"Cannot remove if min != max. This is a bucket!");
				}

				// remove the item we just returned.
				currentBucket.seek(previousIndex);

				// take the last item of the record and put it
				// where we removed the data.
				// if the last item is not the record we are going to remove
				// now.
				long lastItem = currentBucket.length() - recordSize;
				if (lastItem > previousIndex) {
					byte[] lastItemData = new byte[recordSize];
					currentBucket.seek(lastItem);
					currentBucket.readFully(lastItemData);
					// copy the data to the deleted location
					currentBucket.seek(previousIndex);
					currentBucket.write(lastItemData);
				}
				// decrease the size of the file:
				//logger.debug("Length before remove: " + currentBucket.length());
				currentBucket.setLength(currentBucket.length() - recordSize);
				//logger.debug("Length after remove: " + currentBucket.length());
				// restore the pointer
				currentBucket.seek(previousIndex);
				if (currentBucket.length() == 0) {// erase the bucket.
					delete(currentTuple.getKey());
				}
			} catch (Exception e) {
				throw new NoSuchElementException(e.toString());
			}
		}

		@Override
		public void closeCursor() throws OBException {
			it.closeCursor();
			
		}

		/**
		 * Creates a tuple from the given key and value.
		 * 
		 * @param key
		 *            raw key.
		 * @param value
		 *            raw value.
		 * @return A new tuple of type T created from the raw data key and
		 *         value.
		 */
		protected abstract T createTuple(byte[] key, ByteBuffer value);

	}

	/**
	 * Iterator used to process range results.
	 */

	protected class ByteArrayIterator extends CursorIterator<TupleBytes> {

		protected ByteArrayIterator() throws OBStorageException {
			super(null, null, true, false);
		}

		protected ByteArrayIterator(byte[] min, byte[] max)
				throws OBStorageException {
			super(min, max, false, false);
		}

		protected ByteArrayIterator(byte[] min, byte[] max, boolean full,
				boolean backwardsMode) throws OBStorageException {
			super(min, max, full, backwardsMode);
		}

		@Override
		protected TupleBytes createTuple(byte[] key, ByteBuffer value) {
			return new TupleBytes(key, value);
		}
	}

	protected class HandlerLoader implements OBCacheHandlerLong<RandomAccessFileHolder> {

		@Override
		public long getDBSize() throws OBStorageException {
			return storage.size();
		}

		@Override
		public RandomAccessFileHolder loadObject(long key) throws OutOfRangeException,
				OBException, InstantiationException, IllegalAccessException,
				OBStorageException {
			File f = generateBucketFile((int) key);
			try {
				if (!f.getParentFile().exists()) {
					OBAsserts.chkAssert(f.getParentFile().mkdirs(),
							"Could not create all dirs");
				}
				assert key <= Integer.MAX_VALUE;
				return new RandomAccessFileHolder(new RandomAccessFile(f, "rw"));
			} catch (FileNotFoundException e) {
				throw new OBStorageException(e);
			}
		}

		@Override
		public void store(long key, RandomAccessFileHolder object) throws OBException {
			
				object.close();
			

		}

	}
	
	/**
	 * Monitors if the random access file has been closed.
	 * @author amuller
	 *
	 */
	protected final class RandomAccessFileHolder {
		private boolean closed = false;
		private RandomAccessFile f;
		public RandomAccessFileHolder(RandomAccessFile f){
			this.f = f;
		}
		
		public void close() throws OBException{
			closed =  true;
			try{
				f.close();
			}catch(IOException e){
				throw new OBException(e);
			}
		}

		public boolean isClosed() {
			return closed;
		}

		public RandomAccessFile getFile() {
			return f;
		}

	}

	/**
	 * This class holds references to RandomAccessFile. It makes sure that we
	 * 
	 * @author amuller
	 * 
	 */
	protected final class FileHolder {

		private RandomAccessFile file;
		private RandomAccessFileHolder holder;
		private long offset;
		private int bucketId;
		

		public FileHolder(RandomAccessFileHolder h, int bucketId) {
			this.bucketId = bucketId;
			update(h);
			offset = 0;
		}
				
		private void update(RandomAccessFileHolder h){
			this.file = h.getFile();
			this.holder = h;			
		}
		
		/**
		 * Reload the file if it is gone.
		 * @throws IllegalAccessException 
		 * @throws InstantiationException 
		 * @throws OBException 
		 * @throws IOException 
		 * @throws OutOfRangeException 
		 */
		private void verifyFile() throws OBException, IOException{
			try{
			if(holder.isClosed()){
				update(handles.get(bucketId));
			}
			}catch(Exception e){
				throw new OBException(e);
			}
			assert offset <= file.length() : " offset "  + offset + " length " + file.length();
		}
		
		/**
		 * Restore the offset position in the file.
		 * @throws IOException
		 */
		private void updatePrev() throws IOException{
			if(offset != file.getFilePointer()){
				file.seek(offset);
			}
		}
		
		/**
		 * Restore the offset position in the file.
		 * @throws IOException
		 */
		private void updatePos() throws IOException{
			offset = file.getFilePointer();
		}
		public long length() throws IOException, OBException {
			verifyFile();
			return file.length();
		}

		

		public final void readFully(byte[] b) throws IOException, OBException {
			verifyFile();
			updatePrev();
			file.readFully(b);
			updatePos();
		}

		public void seek(long pos) throws IOException, OBException {
			verifyFile();			
			file.seek(pos);
			updatePos();
		}

		public void write(byte[] b) throws IOException, OBException {
			verifyFile();
			updatePrev();
			file.write(b);
			updatePos();
		}

		

		public long getFilePointer() throws IOException, OBException {
			verifyFile();
			return offset;
		}

		public void setLength(long newLength) throws IOException, OBException {
			verifyFile();
			file.setLength(newLength);
		}
		
		

	}

}
