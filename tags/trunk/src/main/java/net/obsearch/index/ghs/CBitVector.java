package net.obsearch.index.ghs;
import java.nio.ByteBuffer;

import net.obsearch.constants.ByteConstants;
import net.obsearch.utils.bytes.ByteConversion;
import cern.colt.bitvector.BitVector;

public final class CBitVector extends BitVector{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CBitVector(int arg0) {
		super(arg0);		
		
	}
	
	public CBitVector(byte[] data, int count){
		super(parseVector(data), count);
		
	}
	
	public CBitVector(BitVector b){
		super(b.elements(), b.size());
	}
	
	private static long[] parseVector(byte[] data){
		ByteBuffer b = ByteConversion.createByteBuffer(data);
		int size = (int)Math.ceil(data.length / ByteConstants.Long.getSize());
		long[] raw = new long[size];
		int i = 0;
		while(i < raw.length){
			raw[i] = b.getLong();
			i++;
		}
		return raw;
	}
	
	public CBitVector copy(){
		return new CBitVector((BitVector)super.clone());
	}

	public int cardinality(){
		int res = 0;
		for(long d : super.bits){
			res += Long.bitCount(d);
		}
		return res;
	}
	
	// perform an xor with the other bit vector
	public int hamming(CBitVector other){
		final long[] theBits = this.bits; // cached for speed.
		final long[] otherBits = other.bits; //cached for speed.
		int res = 0;
		for(int i=theBits.length; --i >= 0;) {
			res += Long.bitCount(theBits[i] ^ otherBits[i]);
		}		
		return res;
	}
	
	public byte[] store(){
		ByteBuffer b = ByteConversion.createByteBuffer(ByteConstants.Long.getSize() * bits.length);
		for(long d : bits){
			b.putLong(d);
		}
		return b.array();
	}
}