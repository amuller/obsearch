package net.obsearch.example.l1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBShort;
import net.obsearch.utils.bytes.ByteConversion;

public class L1 implements OBShort {
	
	private short[] vector;
	
	public L1(){
		// required by OBSearch
	}
	
	public L1(short[] vect){
		this.vector = vect;
	}
	/**
	 * Parses a string with numbers separated by spaces
	 * @param data
	 */
	public L1(String data)throws OBException{
		String[] split = data.split("[ |,]");
		vector = new short[split.length];
		//OBAsserts.chkAssert(vector.length == 64, "Size wrong for vector: " + vector.length);
		
		int i = 0;
		for(String s : split){
			vector[i] = Short.parseShort(s);
			i++;
		}
	}

	@Override
	public short distance(OBShort object) throws OBException {
		L1 other = (L1)object;
		int i = 0;
		int res = 0;
		OBAsserts.chkAssert(vector.length == other.vector.length, "Vector size mismatch");
		while(i < vector.length){
			res += Math.abs(vector[i] - other.vector[i]);
			i++;
		}
		OBAsserts.chkAssert(res <= Short.MAX_VALUE, "short max value exceeded");
		return (short)res; 
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		ShortBuffer s = ByteConversion.createByteBuffer(input).asShortBuffer();

		vector = new short[input.length / ByteConstants.Short.getSize()];

		s.get(vector);
	}

	@Override
	public byte[] store() throws OBException, IOException {
		ByteBuffer b = ByteConversion.createByteBuffer(ByteConstants.Short.getSize() * vector.length);
		ShortBuffer s = b.asShortBuffer();
		s.put(vector);
		return b.array();
	}
	
	public boolean equals(Object o){
		L1 another = (L1)o;
		int i = 0;
		if(this.vector.length != another.vector.length){
			return false;
		}
		while(i < vector.length){
			if(vector[i] != another.vector[i]){
				return false;
			}
			i++;
		}
		return true;
	}

}
