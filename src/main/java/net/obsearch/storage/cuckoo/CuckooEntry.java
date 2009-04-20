package net.obsearch.storage.cuckoo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import net.obsearch.Storable;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.storage.OBStore;
import net.obsearch.utils.bytes.ByteConversion;

public class CuckooEntry implements Storable{
	
	
	private byte[] key;
	private byte[] value;
	
	// <next, long> <key, short> <value, int> <byte[] key> <byte[] value> 
	private static final int HEADER_SIZE = ByteConstants.Long.getSize() + ByteConstants.Short.getSize();
	
	private long next = -1;
	
	/**
	 * The id of the entry in the byte array.
	 */
	private long id;
	
	public CuckooEntry(long id){
		this.id = id;
	}
	
	
			
	public long getId() {
		return id;
	}



	public CuckooEntry(byte[] key,  byte[] value) throws OBException {		
		super();
		OBAsserts.chkAssert(key.length <= Short.MAX_VALUE, "Keys cannot be bigger than 2^16");
		OBAsserts.chkAssert(value.length <= Integer.MAX_VALUE, "Values cannot be bigger than 2^32");
		this.key = key;
		this.value = value;		
	}
	
	public boolean hasNext(){
		return next != -1;
	}
	
	public boolean equals(Object other){
		CuckooEntry c = (CuckooEntry)other;
		return Arrays.equals(key, c.key) && Arrays.equals(value, c.value);
	}

	/**
	 * Serialize the cuckoo entry into a byte[]
	 */
	public byte[] store() throws IOException{
		 ByteBuffer buf = ByteConversion.createByteBuffer(HEADER_SIZE + key.length + value.length );
		 buf.putLong(next);
		 buf.putShort((short)key.length);		 
		 buf.put(key);
		 buf.put(value);
		 return buf.array();		 
	}
	
	/**
	 * Return the next object.
	 * @return
	 */
	public long getNext(){
		return next;
	}
	
	/**
	 * Load the cuckoo entry from the given array.
	 */
	public void load(byte[] input) throws IOException{
		ByteBuffer buf = ByteConversion.createByteBuffer(input);
		next = buf.getLong();
		short keySize = buf.getShort();
		key = new byte[keySize];
		value = new byte[input.length - keySize - HEADER_SIZE];
		buf.get(key);
		buf.get(value);		
	}
		
	/**
	 * Set the next object.
	 * @param next
	 */
	public void setNext(long next){
		this.next = next;
	}


	
	public byte[] getKey() {
		return key;
	}
	public void setKey(byte[] key) {
		this.key = key;
	}
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	

}
