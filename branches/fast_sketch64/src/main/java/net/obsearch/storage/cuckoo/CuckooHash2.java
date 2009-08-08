package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * A secondary storage cuckoo hash implementation of one move. This implementation is
 * designed to allow fast bulk insertions. It assumes that you know 
 * well in advance the number of items that will be added. 
 * id is the hash code or the index in the bucket array.
 * position is the index in the CuckooRecordManager
 * @author Arnoldo Jose Muller-Molina
 * 
 */
public class CuckooHash2 implements HardDiskHash{

	/**
	 * Hash number 1.
	 */
	private FixedPointerTable h1;

	/**
	 * Hash number2.
	 */
	private FixedPointerTable h2;
	
	/**
	 * Data is stored here.
	 */
	private DiskHeap rec;
	
	private CuckooHashStats stats = new CuckooHashStats() ;
	
	
	
	/**
	 * Hash function 1.
	 */
	private HashFunction f1;
	/**
	 * Hash function 2.
	 */
	private HashFunction f2;

	/**
	 * Number of buckets per hash function.
	 */
	private int bucketNumber;
	
	
	private RandomAccessFile count;
	
	private long countCache;

	/**
	 * Create a new hash with the expected # of objects to be added into the
	 * hash.
	 * 
	 * @param expectedObjects
	 *            Number of objects that will be added
	 * @param directory
	 *            The directory in which the objects will be added.
	 * @throws IOException
	 * @throws OBException 
	 */
	public CuckooHash2(int bucketNumber, File directory, HashFunction f1, HashFunction f2)
			throws IOException, OBException {
		OBAsserts.chkAssert(bucketNumber > 0, "You must insert at least 1 object");
		directory.mkdirs();
		this.bucketNumber = bucketNumber;
		rec = new DiskHeap(directory);
		h1 = new FixedPointerTable(new File(directory, "h1.az"), bucketNumber);		
		h2 = new FixedPointerTable(new File(directory, "h2.az"), bucketNumber);		
		this.f1 = f1;
		this.f2 = f2;
		
		count = new RandomAccessFile(
				new File(directory, "_count.az"), "rw");
		
		countCache = getCount();
	}
	
		
	
	public CuckooHashStats getStats() throws IOException, OBException{
		stats.setFragReport(rec.fragmentationReport());
		return stats;
	}
	
	private long getCount() throws IOException{
		if(count.length() == ByteConstants.Long.getSize()){
			count.seek(0);
			return count.readLong();
			
		}else{
			return 0;
		}
	}
	
	private void putCount(long c) throws IOException{
		count.seek(0);
		count.writeLong(c);
	}
	
	
	/**
	 * Add the container to the storage and the given fixed pointer table.
	 * @param e
	 * @param cont
	 * @throws IOException 
	 * @throws OBException 
	 */
	private void putAux(int hashCode, FixedPointerTable t, Entry e, CuckooEntryContainer cont) throws OBException, IOException{
		byte[] data = cont.store();
		if(! e.isNull()){
			// release old address
			rec.release(e.getOffset(), e.getLength());
			stats.incMemReleases();
			// if data.length and e.getLength() happened to be the same
			// the heap will recover immediately the same space :)
		}		
		long offset = rec.store(data);
		stats.incMemRequests();
		if(offset == e.getOffset()){
			stats.incMemRecycles();
		}
		Entry toStore = new Entry(offset, data.length);		
		t.set(hashCode, toStore);
	}
	/**
	 * Add put a key in the hash table.
	 * @param key
	 * @param value
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void put(byte[] key, byte[] value) throws IOException, OBException{
				
		int hash1 = getH1Hash(key);
		Entry e1 = h1.get(hash1);
		CuckooEntryCompact toStore = new CuckooEntryCompact(key,value);
		CuckooEntryContainer cont1 = new CuckooEntryContainer();
		if(e1.isNull()){
			// lucky it is a new bucket.
			cont1.add(toStore);
			putAux(hash1, h1, e1, cont1);
			stats.incH1Inserts();
			countCache++;
			return;
		}else{
			// not null we must check if the same key is there.
			cont1 = getContainer(e1);
			CuckooEntryCompact sameKey1 = cont1.search(key);
			if(sameKey1 != null){
				// key is in the bucket, update keys.
				sameKey1.setValue(value);				
				putAux(hash1, h1, e1, cont1);	
				stats.incH1Inserts();
				return; // done
			}
			
			// check the other hash table
			int hash2 = getH2Hash(key);
			Entry e2 = h2.get(hash2);
			CuckooEntryContainer cont2 = new CuckooEntryContainer();
			
			if(e2.isNull()){
				// lucky it is a new bucket
				cont2.add(toStore);
				putAux(hash2, h2, e2, cont2);	
				stats.incH2Inserts();
				countCache++;
				return;
			}else{
				// both buckets have data. 
				// we have to load both containers.				
				cont2 = getContainer(e2);
				CuckooEntryCompact sameKey2 = cont2.search(key);
				// check if the key exists
				if(sameKey2 != null){
					// key is in the bucket, update keys.
					sameKey2.setValue(value);				
					putAux(hash2, h2, e2, cont2);
					stats.incH2Inserts();
					return; // done
				}
				
				// key does not exist, add the object in the smallest
				// container.
				if(cont2.size() < cont1.size()){
					cont2.add(toStore);
					putAux(hash2, h2, e2, cont2);	
					stats.incH2Inserts();
				}else{
					cont1.add(toStore);
					putAux(hash1, h1, e1, cont1);
					stats.incH1Inserts();
				}
				countCache++;
				return;
			}
		}
		
		
	}
	
	private CuckooEntryContainer getContainer(Entry e) throws IOException, OBException{
		if(e.isNull()){
			return null;
		}
		byte[] buf = new byte[e.getLength()];
		rec.read(e.getOffset(), buf);
		CuckooEntryContainer cont = new CuckooEntryContainer();
		cont.load(buf);
		return cont;
	}
	
	
	public byte[] get(byte[] key) throws IOException, OBException{
		CuckooEntryCompact result = getAux(key);
		if(result != null){
			return result.getValue();
		}else{
			return null;
		}
	}
	
	/**
	 * Delete the given key.
	 * @param key
	 * @return
	 * @throws OBException 
	 * @throws IOException 
	 */
	public boolean delete(byte[] key) throws IOException, OBException{
		int hash1 = getH1Hash(key);
		Entry e1 = h1.get(hash1);
		if(! e1.isNull()){
			CuckooEntryContainer c1  = getContainer(e1);
			CuckooEntryCompact bucket1 = c1.search(key);
			if(bucket1 != null){
				c1.delete(key);
				// save the results.
				putAux(hash1, h1, e1, c1);
				countCache++;
				return true;
			}else{
				// we have to search the other hash
				int hash2 = getH1Hash(key);
				Entry e2 = h2.get(hash2);
				if(! e2.isNull()){
					// the container exist
					CuckooEntryContainer c2  = getContainer(e2);
					CuckooEntryCompact bucket2 = c2.search(key);
					// and it has the key we want.
					if(bucket2 != null){
						c2.delete(key);
						// save the results.
						putAux(hash2, h2, e2, c2);
						countCache++;
						return true;
					}
				}
			}
		}
								
		return false;
	}
	
	public void deleteAll() throws IOException{
		rec.deleteAll();
		countCache = 0;
		putCount(0);
	}
	
	private CuckooEntryCompact getAux(byte[] key) throws IOException, OBException{
		int hash1 = getH1Hash(key);
		
		Entry e1 = h1.get(hash1);
		CuckooEntryCompact res = null;
		CuckooEntryContainer result = getContainer(e1);
		
		if(result != null){
			res = result.search(key);
		}
		
		if(res == null){
			int hash2 = getH2Hash(key);
			Entry e2 = h2.get(hash2);
			result = getContainer(e2);
			if(result != null){
				res = result.search(key);
			}
			
		}
		if(result != null){
			stats.addGetLength(result.size());
		}
		return res;
	}
	
	
	
	
	
	private int getH1Hash(byte[] key){		
		return getHashAux(key, f2);
	}
	
	private int getH2Hash(byte[] key){
		return getHashAux(key, f2);
	}
	private int getHashAux(byte[] key, HashFunction func){
		long res = Math.abs(func.compute(key) % expectedNumberOfObjects());
		assert res >= 0 : "something went wrong, " + res;
		return (int)res;
	}
	
	private int expectedNumberOfObjects(){
		return this.bucketNumber;
	}

	public long size() throws IOException {
		return countCache;
	}
	
	public void close() throws IOException{
		h1.close();
		h2.close();
		rec.close();
		putCount(countCache);
	}
	
	/**
	 * Iterator of all the keys and values of the hash table.
	 * @return
	 * @throws IOException 
	 * @throws OBException 
	 */
	public CloseIterator<TupleBytes> iterator() throws OBException, IOException{
		return new CH2Iterator();
	}
	
	private class CH2Iterator implements CloseIterator<TupleBytes>{
		private Iterator<Entry> currentHash;
		private Iterator<TupleBytes> currentBucketContainer;
		private TupleBytes next;
		private int hashNumber = 1;
		
		public CH2Iterator() throws OBException, IOException{
			currentHash = h1.iterator();
		
			calculateNext();
		}
		
		private void calculateNext() throws NoSuchElementException{
			try{
			next = null; 
			if(currentBucketContainer == null || ! currentBucketContainer.hasNext() ){
				nextBucketContainer();
			}
			
			if(currentBucketContainer != null && currentBucketContainer.hasNext()){
				next = currentBucketContainer.next();
			}
			}catch(Exception e){
				e.printStackTrace();
				throw new NoSuchElementException(e.toString());
			}
		}
		
		private void nextBucketContainer() throws OBException, IOException{
			currentBucketContainer = null;
			if(! currentHash.hasNext()){
				if(hashNumber == 1){
					hashNumber++;
					currentHash = h2.iterator();
				}
			}
			
			if(currentHash.hasNext()){
				Entry e = currentHash.next();
				currentBucketContainer = getContainer(e).iterator();
				assert currentBucketContainer.hasNext();
			}
		}

		@Override
		public boolean hasNext() {
			return next != null;
		}

		@Override
		public TupleBytes next() {
			TupleBytes res = next;
			calculateNext();
			return res;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
			
		}

		@Override
		public void closeCursor() throws OBException {
			// TODO Auto-generated method stub
			
		}
		
	}

}
