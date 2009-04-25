package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleLong;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * This class is like a byte[][] in secondary storage. You can store byte[] of
 * arbitrary size based on index. You can also delete objects but the chances of
 * fragmentation do exist. The size() operation returns the size of the array (including null cells)
 * 
 * 
 */
public class ByteArrayFlex implements ByteArray {

	/**
	 * Where all the data is located
	 */
	private RandomAccessFile main;

	/**
	 * We keep track of the entries here.
	 */
	private PointerTable entries;

	/**
	 * We keep track of released space here.
	 */
	private PointerTable holes;
	
	

	/**
	 * Create a new byte array storage in the given folder
	 * 
	 * @param in
	 * @throws FileNotFoundException
	 * @throws OBStorageException 
	 */
	public ByteArrayFlex(File in) throws  OBStorageException {
		try{
		RandomAccessFile mainF = new RandomAccessFile(new File(in, "main.az"),
				"rw");
		RandomAccessFile entriesF = new RandomAccessFile(new File(in,
				"entries.az"), "rw");
		RandomAccessFile holesF = new RandomAccessFile(
				new File(in, "holes.az"), "rw");
		
		main = mainF.getChannel();
		entries = new PointerTable(entriesF.getChannel());
		holes = new PointerTable(holesF.getChannel());
		
		}catch(FileNotFoundException e){
			throw new OBStorageException(e);
		}
		
		
	}

	/* (non-Javadoc)
	 * @see net.obsearch.storage.cuckoo.ByteArray#put(long, byte[])
	 */
	public void put(long id, byte[] data) throws OBException, IOException {
		OBAsserts
				.chkAssert(id <= entries.size(),
						"Cannot exceed the size of the array, extend the array by using 'add'.");
		if(id == size()){
			OBAsserts.chkAssert(id == add(data), "Bad id, fatal error"); // create a new element at the end.
		}
		Entry e = entries.get(id);
		if (e.getLength() == data.length) { // little optimization
			// lucky, we just put the object where it was.
			store(e, data);
		} else {
			// we have to allocate new space.
			// find suitable space in the list of empty buckets.
			long i = 0;
			Entry newEntry = null;
			while (i < holes.size()) {
				Entry hole = holes.get(i);
				if (hole.getLength() == data.length) {
					// we found our hole, delete it from the list of holes
					holes.delete(i);
					// we found a hole!
					newEntry = hole;
					break;
				}
				i++;
			}
			if (newEntry == null) {
				// we did not find a hole, let's create a new one at the end of
				// main memory.
				newEntry = new Entry(sizeBytes(), data.length);
			}
			// store our new entry.
			entries.set(id, newEntry);
			// put our data.
			store(newEntry, data);
			// if the previous entry is not null
			addToHoleSet(e);
		}
	}

	/* (non-Javadoc)
	 * @see net.obsearch.storage.cuckoo.ByteArray#get(long)
	 */
	public byte[] get(long i) throws OBException, IOException {
		Entry e = entries.get(i);
		if (e.isNull()) {
			return null;
		}
		ByteBuffer data = ByteConversion.createByteBuffer(e.getLength());
		main.read(data, e.getOffset());
		return data.array();
	}

	public void close() throws IOException {
		main.close();
		entries.close();
		holes.close();
	}

	/* (non-Javadoc)
	 * @see net.obsearch.storage.cuckoo.ByteArray#delete(long)
	 */
	public boolean delete(long i) throws OBException, IOException {
		// get the entry to store it in the holes section
		Entry e = entries.get(i);
		boolean result = e.isNull();
		addToHoleSet(e);
		// store back the nullified entry
		e.setNull();
		entries.set(i, e);
		return result;
	}

	private void addToHoleSet(Entry e) throws IOException {
		if (!e.isNull()) {
			holes.add(e);
		}
	}

	/* (non-Javadoc)
	 * @see net.obsearch.storage.cuckoo.ByteArray#add(byte[])
	 */
	public long add(byte[] data) throws IOException {
		Entry newEntry = new Entry(sizeBytes(), data.length);
		entries.add(newEntry);
		store(newEntry, data);
		return entries.size() - 1;
	}

	/* (non-Javadoc)s
	 * @see net.obsearch.storage.cuckoo.ByteArray#size()
	 */
	public long size() throws IOException {
		return entries.size();
	}

	public long sizeBytes() throws IOException {
		return main.size();
	}

	/**
	 * store the given data in the main array.
	 * 
	 * @param position
	 * @param data
	 * @throws IOException
	 */
	private void store(Entry entry, byte[] data) throws IOException {
		ByteBuffer buf = ByteConversion.createByteBuffer(data);
		main.write(buf, entry.getOffset());
	}

	/**
	 * Generate a report with the sizes of fragmented files.
	 * 
	 * @return
	 * @throws IOException
	 * @throws OBException
	 */
	public StaticBin1D fragmentationReport() throws IOException, OBException {
		StaticBin1D result = new StaticBin1D();
		long i = 0;
		while (i < holes.size()) {
			Entry e = holes.get(i);
			result.add(e.getLength());
			i++;
		}
		return result;
	}

	public void defrag() {
		throw new IllegalArgumentException("Sorry, not yet implemented");
	}

	

	/* (non-Javadoc)
	 * @see net.obsearch.storage.cuckoo.ByteArray#iterator()
	 */
	public CloseIterator<TupleLong> iterator() {

		return new ByteArrayStorageIterator(0);

	}
	
	public void deleteAll() throws IOException{
		this.holes.deleteAll();
		this.entries.deleteAll();
		this.main.truncate(0);
	}

	public class ByteArrayStorageIterator implements CloseIterator<TupleLong> {

		private long nextPointer;
		private TupleLong next;

		/**
		 * Create an iterator starting from "startPosition"
		 * 
		 * @param startPosition
		 */
		public ByteArrayStorageIterator(long startPosition) {
			nextPointer = startPosition;
			calculateNext();
		}
		// only returns non-null objects.
		private void calculateNext() {
			try {
				byte[] obj = null;
				long loadedPointer = -1;
				while(obj == null && nextPointer < size()) {
					obj = get(nextPointer);
					loadedPointer = nextPointer;
					nextPointer++; // leaves the pointer ready for the next
									// iteration.
				}
				if (obj == null) {
					next = null; // end everything
				} else {
					next = new TupleLong(loadedPointer, obj);
				}
			} catch (IOException e) {
				throw new NoSuchElementException(e.toString());
			} catch (OBException e) {
				throw new NoSuchElementException(e.toString());
			}
		}

		@Override
		public boolean hasNext() {
			//try{
				return next != null;
			//}catch(IOException e){
			//	throw new NoSuchElementException(e.toString());
			//}
			
		}

		@Override
		public void closeCursor() throws OBException {

		}
		
		@Override
		public TupleLong next() {			
			TupleLong toReturn = next;
			calculateNext();
			return toReturn;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();			
		}
		
		

	}

}
