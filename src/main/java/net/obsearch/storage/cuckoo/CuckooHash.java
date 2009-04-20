package net.obsearch.storage.cuckoo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * A secondary storage cuckoo hash implementation of one move. This implementation is
 * designed to allow fast bulk insertions. It assumes that you know 
 * well in advance the number of items that will be added. 
 * 
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
		directory.mkdirs();
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
		
		h1F.setLength(expectedNumberOfObjects * ByteConstants.Long.getSize());
		h2F.setLength(expectedNumberOfObjects * ByteConstants.Long.getSize());
		h1 = h1F.getChannel();
		h2 = h2F.getChannel();
				
		
		this.expectedNumberOfObjects = expectedNumberOfObjects;
		this.f1 = f1;
		this.f2 = f2;
	}
	
	/**
	 * Add put a key in the hash table.
	 * @param key
	 * @param value
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void put(byte[] key, byte[] value) throws IOException, OBException{
		long k1 = getH1Hash(key);
		long position = getPosition(h1, k1);
		if(position == 0){
			// lucky, it is a new bucket :)
			long id = rec.addEntry(new CuckooEntry(key,value));
			putPosition(h1, position, id);
			// done.
		}else{
			// it is not a new bucket, we have to get the position.
			CuckooEntry e = rec.getEntry(position);
			// apply the hash to this guy. (the egg we are trying to throw)
			long poorEggHashCode = getH2Hash(key);
			// get the new position of the poor egg.
			long positionPoorEgg = getPosition(h2, poorEggHashCode);
			if(positionPoorEgg == 0){
				// safe, poor lucky Egg! 
				putPosition(h2, poorEggHashCode, position);
			}else{
				// we got into a cell that has another duplicate
				// delete the poorEgg
				rec.delete(i)
			}
		}
	}
	
	/**
	 * Get the cell # pos
	 * @param ch
	 * @param pos
	 * @return
	 * @throws IOException 
	 */
	private long getPosition(FileChannel ch, long pos) throws IOException{
		ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Long.getSize());
		ch.read(buf, pos * ByteConstants.Long.getSize());
		buf.rewind();
		return buf.getLong();
	}
	/**
	 * Put the object in the given position.
	 * @param ch
	 * @param pos
	 * @param value
	 * @return
	 * @throws IOException
	 */
	private void putPosition(FileChannel ch, long pos, long value) throws IOException{
		ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Long.getSize());
		buf.putLong(value);
		buf.rewind();
		ch.write(buf, pos * ByteConstants.Long.getSize());
		
	}
	
	private long getH1Hash(byte[] key){
		long res = f1.compute(key) % expectedNumberOfObjects();
		assert res >= 0;
		return res;
	}
	
	private long getH2Hash(byte[] key){
		long res = f2.compute(key) % expectedNumberOfObjects();
		assert res >= 0;
		return res;
	}
	
	private long expectedNumberOfObjects(){
		return this.expectedNumberOfObjects;
	}

	public long size() throws IOException {
		return rec.size();
	}

}
