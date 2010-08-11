package net.obsearch.index.utils;

import java.nio.ByteBuffer;

import cern.colt.bitvector.BitVector;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.utils.bytes.ByteConversion;

public class BitBucket {

	private BitVector data;
	
	public BitBucket(){
		
	}
	/**
	 * Create a new bit bucket with the given
	 * number of bits.
	 * @param size
	 */
	public BitBucket(int size){
		data = new BitVector(size);
	}

	public void clear(int arg0) {
		data.clear(arg0);
	}

	public void set(int arg0) {
		data.set(arg0);
	}
	
	public BitBucket copy(){
		BitBucket res = new BitBucket();
		res.data = this.data.copy();
		return res;
	}
	
	public byte[] getBytes(){
		ByteBuffer res = ByteConversion.createByteBuffer(ByteConstants.Long.getSize() * data.elements().length);
		for(long l : data.elements()){
			res.putLong(l);
		}
		return res.array();
	}
}
