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
public class CuckooHash2 {

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
		rec = new DiskHeap(new File(directory, "heap.az"));
		h1 = new FixedPointerTable(new File(directory, "h1.az"), bucketNumber);		
		h2 = new FixedPointerTable(new File(directory, "h2.az"), bucketNumber);		
		this.f1 = f1;
		this.f2 = f2;
	}
	
		
	
	public CuckooHashStats getStats() throws IOException, OBException{
		stats.setFragReport(rec.fragmentationReport());
		return stats;
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
			// if data.length and e.getLength() happened to be the same
			// the heap will recover immediately the same space :)
		}		
		long offset = rec.store(data);
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
		}else{
			// not null we must check if the same key is there.
			cont1 = getContainer(e1);
			CuckooEntryCompact sameKey1 = cont1.search(key);
			if(sameKey1 != null){
				// key is in the bucket, update keys.
				sameKey1.setValue(value);				
				putAux(hash1, h1, e1, cont1);
				return;
			}
			
			// check the other hash table
			int hash2 = getH2Hash(key);
			Entry e2 = h2.get(hash2);
			CuckooEntryContainer cont2 = new CuckooEntryContainer();
			
			if(e2.isNull()){
				// lucky it is a new bucket
				cont2.add(toStore);
				putAux(hash2, h2, e2, cont2);				
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
					return;
				}
				
				// key does not exist, add the object in the smallest
				// container.
				if(cont2.size() < cont1.size()){
					cont2.add(toStore);
					putAux(hash2, h2, e2, cont2);					
				}else{
					cont1.add(toStore);
					putAux(hash1, h1, e1, cont1);
				}
			}
		}
		
		
	}
	
	private CuckooEntryContainer getContainer(Entry e) throws IOException, OBException{
		byte[] buf = new byte[e.getLength()];
		rec.read(e.getOffset(), buf);
		CuckooEntryContainer cont = new CuckooEntryContainer();
		cont.load(buf);
		return cont;
	}
	
	
	public byte[] get(byte[] key) throws IOException, OBException{
		CuckooEntry result = getAux(key);
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
		CuckooEntry e = getAux(key);
		if(e != null){
			rec.delete(e.getId());
			return true;
		}
		return false;
	}
	
	public void deleteAll() throws IOException{
		rec.deleteAll();

	}
	
	private CuckooEntryCompact getAux(byte[] key) throws IOException, OBException{
		int hash1 = getH1Hash(key);		
		Entry e1 = h1.get(hash1);
		
		CuckooEntryCompact result = getContainer(e1).search(key); 
		if(result == null){
			int hash2 = getH2Hash(key);
			Entry e2 = h2.get(hash2);
			result = getContainer(e2).search(key); 
			
		}
		return result;
	}
	
	/**
	 * Return the byte value of the given index in the FileChannel
	 * @param id
	 * @param ch
	 * @param key
	 * @return null if the key is not found.
	 * @throws OBException 
	 * @throws IOException 
	 */
	private CuckooEntry getFromIndex(long id, MappedByteBuffer ch, byte[] key) throws IOException, OBException{
		long pos =  getPosition(ch, id);
		if( pos == -1){
			return null;
		}
		List<CuckooEntry> ent = rec.getEntrySequence(pos);
		stats.addGetLength(ent.size());
		for(CuckooEntry e : ent){
			if(Arrays.equals(e.getKey(), key)){
				// return the value if the keys match.
				return e;
			}
		}
		return null;
	}
	
	/**
	 * Get the cell # pos
	 * @param ch
	 * @param pos
	 * @return
	 * @throws IOException 
	 * @throws OBException 
	 */
	private long getPosition(MappedByteBuffer ch, long id) throws IOException, OBException{
		//ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Long.getSize());
		//ch.read(buf, id * ByteConstants.Long.getSize());
		//buf.rewind();
		int pos = convertId(id);
		
		return ch.getLong(pos);
	}
	
	private int convertId(long id) throws OBException{
		long pos = (id * ByteConstants.Long.getSize());
		OBAsserts.chkAssert(pos <= Integer.MAX_VALUE, "Cannot exceed 2^32");
		return (int)pos;
	}
	/**
	 * Put the object in the given position.
	 * @param ch
	 * @param id
	 * @param position
	 * @return
	 * @throws IOException
	 * @throws OBException 
	 */
	private void putPosition(MappedByteBuffer ch, long id, long position) throws IOException, OBException{
		//ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Long.getSize());
		//buf.putLong(position);
		//buf.rewind();
		int pos = convertId(id);;
		ch.putLong((int) pos , position);

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
		return rec.size();
	}
	
	public void close() throws IOException{
		h1.close();
		h2.close();
		rec.close();
	}
	
	/**
	 * Iterator of all the keys and values of the hash table.
	 * @return
	 */
	public CloseIterator<TupleBytes> iterator(){
		return (CloseIterator)rec.iterator();
	}

}
