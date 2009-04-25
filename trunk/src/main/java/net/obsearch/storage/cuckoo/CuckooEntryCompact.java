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
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;

public class CuckooEntryCompact extends TupleBytes  {
	
	
	
	
	private static final int HEADER_SIZE = ByteConstants.Int.getSize() + ByteConstants.Short.getSize();
	
	
	
	public CuckooEntryCompact(byte[] key,  byte[] value) throws OBException {		
		super(key,value);
		OBAsserts.chkAssert(key.length <= Short.MAX_VALUE, "Keys cannot be bigger than 2^16");
		OBAsserts.chkAssert(value.length <= Integer.MAX_VALUE, "Values cannot be bigger than 2^32");
	}
	
	public CuckooEntryCompact(){
		
	}
	
	
	/**
	 * Serialized size of the object
	 * @return
	 */
	public int serializedSize(){
		return HEADER_SIZE + key.length + value.length;
	}
	
	
	
	public boolean equals(Object other){
		CuckooEntryCompact c = (CuckooEntryCompact)other;
		return Arrays.equals(key, c.key) && Arrays.equals(value, c.value);
	}

	/**
	 * Serialize the cuckoo entry into a ByteBuffer
	 */
	public void store(ByteBuffer buf) throws IOException{
		 buf.putShort((short)key.length);
		 buf.put(key);
		 buf.putInt(value.length);
		 buf.put(value);
	}
	

	/**
	 * Load the cuckoo entry from the given array.
	 */
	public void load(ByteBuffer buf) throws IOException{		
		short keySize = buf.getShort();
		key = new byte[keySize];
		buf.get(key);
		int valueSize = buf.getInt();
		value = new byte[valueSize];
		buf.get(value);		
	}
		
	
	

}
