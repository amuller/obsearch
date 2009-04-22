package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
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
public class CuckooHash {

	/**
	 * Hash number 1.
	 */
	private FileChannel h1;

	/**
	 * Hash number2.
	 */
	private FileChannel h2;

	/**
	 * Manager!
	 */
	private CuckooRecordManager rec;
	
	private CuckooHashStats stats = new CuckooHashStats() ;
	
	
	
	/**
	 * Hash function 1.
	 */
	private HashFunction f1;
	/**
	 * Hash function 2.
	 */
	private HashFunction f2;

	private long expectedNumberOfObjects;

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
	public CuckooHash(long expectedNumberOfObjects, File directory, HashFunction f1, HashFunction f2)
			throws IOException, OBException {
		OBAsserts.chkAssert(expectedNumberOfObjects > 0, "You must insert at least 1 object");
		directory.mkdirs();
		this.expectedNumberOfObjects = expectedNumberOfObjects;
		rec = new CuckooRecordManager(directory);
		RandomAccessFile h1F = new RandomAccessFile(
				new File(directory, "h1.az"), "rw");
		RandomAccessFile h2F = new RandomAccessFile(
				new File(directory, "h2.az"), "rw");
		OBAsserts.chkAssert(
				expectedNumberOfObjects == (h1F.length() / ByteConstants.Long
						.getSize()) || h1F.length() == 0,
				"Expected objects and current objects mismatch: "
						+ expectedNumberOfObjects + " current: "
						+ (h1F.length() / ByteConstants.Long.getSize()));
		boolean create = h1F.length() == 0;
			
		
		h1F.setLength(expectedNumberOfObjects * ByteConstants.Long.getSize());
		h2F.setLength(expectedNumberOfObjects * ByteConstants.Long.getSize());
		h1 = h1F.getChannel();
		h2 = h2F.getChannel();
		if(create){
			init(h1);
			init(h2);
		}
				
		// must initialize h1 and h2 with -1
		this.expectedNumberOfObjects = expectedNumberOfObjects;
		this.f1 = f1;
		this.f2 = f2;
	}
	
	/**
	 * Init a channel with -1s
	 * @param ch
	 * @throws IOException 
	 */
	private void init(FileChannel ch) throws IOException{
		long i = 0;
		while(i < expectedNumberOfObjects()){
			 putPosition(ch, i, -1);
			i++;
		}
	}
	
	
	public CuckooHashStats getStats() throws IOException, OBException{
		stats.setFragReport(rec.fragmentationReport());
		return stats;
	}
	
	/**
	 * Add put a key in the hash table.
	 * @param key
	 * @param value
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void put(byte[] key, byte[] value) throws IOException, OBException{
				
		
		long id1 = getH1Hash(key);
		long position = getPosition(h1, id1);
		CuckooEntry toStore = new CuckooEntry(key,value);
		if(position == -1){
			// lucky, it is a new bucket :)
			position = rec.addEntry(toStore);
			putPosition(h1, id1, position);
			stats.incH1Inserts();
			stats.addDepth(0);
			// done.
		}else{
			// we have to consider the case where the key exists in the DB.
			List<CuckooEntry> h1Guys = rec.getEntrySequence(position);
			// search if the key matches any of them.
			CuckooEntry searchingKey = searchKey(key, h1Guys);
			// if someone matches, do an override
			if(searchingKey != null){
				searchingKey.setValue(value); // add the new value.
				// overwrite the previous item.
				rec.putEntry(searchingKey.getId(), searchingKey);
				return; // added object, job done.
			}
			
			// not so lucky, we go to h2
			// apply the hash to this poor guy. (the egg we are trying to throw)
			long poorEggId2 = getH2Hash(key);
			// get the new position of the poor egg.
			long poorEggPosition = getPosition(h2, poorEggId2);
			if(poorEggPosition == -1){
				// safe, we can store the poor little egg safely
				long position2 = rec.addEntry(toStore);
				putPosition(h2, poorEggId2, position2);
				stats.incH2Inserts();
				stats.addDepth(0);
			}else{
				// both buckets are full, crap we will have to add objects to the buckets.
				
				// get the other guys
				List<CuckooEntry> h2Guys = rec.getEntrySequence(poorEggPosition);
				// search if the key matches any of them.
				searchingKey = searchKey(key, h2Guys);
				// if someone matches, do an override
				if(searchingKey != null){
					searchingKey.setValue(value); // add the new value.
					// overwrite the previous item.
					rec.putEntry(searchingKey.getId(), searchingKey);
					return; // added object, job done.
				}
				// both buckets are full and our key is not included in any of them
				// we have to add our object to the end of the list.
				
				if(h1Guys.size() < h2Guys.size()){
					// store in h1Guys because they are smaller.
					CuckooEntry last = h1Guys.get(h1Guys.size() - 1);
					rec.putEntrySequence(last.getId(), toStore);
					stats.incH1Inserts();
					stats.addDepth(h1Guys.size());
				}else{
					CuckooEntry last = h2Guys.get(h2Guys.size() - 1);
					rec.putEntrySequence(last.getId(), toStore);
					stats.incH2Inserts();
					stats.addDepth(h2Guys.size());
				}
			}
		}
	}
	
	/**
	 * Search key in the given list of entries.
	 * @param key key to search
	 * @param entries the entries to process.
	 * @return null if not found, otherwise the entry with an equivalent key.
	 */
	private CuckooEntry searchKey(byte[] key, List<CuckooEntry> entries){
		for(CuckooEntry e : entries){
			if(Arrays.equals(key, e.getKey())){
				return e;
			}
		}
		return null;
	}
	
	
	public byte[] get(byte[] key) throws IOException, OBException{
		CuckooEntry result = getAux(key);
		if(result != null){
			return result.getValue();
		}else{
			return null;
		}
	}
	
	private CuckooEntry getAux(byte[] key) throws IOException, OBException{
		long id1 = getH1Hash(key);
		long id2 = getH2Hash(key);
		CuckooEntry result = getFromIndex(id1, h1, key);
		if(result == null){
			result = getFromIndex(id2, h2, key);
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
	private CuckooEntry getFromIndex(long id, FileChannel ch, byte[] key) throws IOException, OBException{
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
	 */
	private long getPosition(FileChannel ch, long id) throws IOException{
		ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Long.getSize());
		ch.read(buf, id * ByteConstants.Long.getSize());
		buf.rewind();
		return buf.getLong();
	}
	/**
	 * Put the object in the given position.
	 * @param ch
	 * @param id
	 * @param position
	 * @return
	 * @throws IOException
	 */
	private void putPosition(FileChannel ch, long id, long position) throws IOException{
		ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Long.getSize());
		buf.putLong(position);
		buf.rewind();
		ch.write(buf, id * ByteConstants.Long.getSize());
		
	}
	
	private long getH1Hash(byte[] key){		
		return getHashAux(key, f2);
	}
	
	private long getH2Hash(byte[] key){
		return getHashAux(key, f2);
	}
	private long getHashAux(byte[] key, HashFunction func){
		long res = Math.abs(func.compute(key) % expectedNumberOfObjects());
		assert res >= 0 : "something went wrong, " + res;
		return res;
	}
	
	private long expectedNumberOfObjects(){
		return this.expectedNumberOfObjects;
	}

	public long size() throws IOException {
		return rec.size();
	}
	
	public void close() throws IOException{
		this.h1.close();
		this.h2.close();
		rec.close();
	}
	
	/**
	 * Iterator of all the keys and values of the hash table.
	 * @return
	 */
	public Iterator<CuckooEntry> iterator(){
		return rec.iterator();
	}

}
