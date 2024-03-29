package net.obsearch.example.vectors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBInt;
import net.obsearch.ob.OBShort;


public class L1 implements OBShort {
	
	private short[] vector;
	
	public L1(){
		// required by OBSearch
	}
	
	/**
	 * Construct an object from an array.
	 * @param vector
	 */
	public L1(short[] vector){
		this.vector = vector;
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
		OBAsserts.chkAssert(res <= Short.MAX_VALUE, "max value exceeded");
		return (short)res; 
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		ShortBuffer s = ByteBuffer.wrap(input).asShortBuffer();
		vector = new short[input.length / ByteConstants.Short.getSize()];
		s.get(vector);
		/*ByteArrayInputStream in = new ByteArrayInputStream(input);
		DataInputStream i = new DataInputStream(in);
		int max = input.length / ByteConstants.Short.getSize();
		vector = new short[max];
		int cx = 0;
		while(cx < max){
			vector[cx] = i.readShort();
			cx++;
		}
		i.close();		*/
	}
	
	/**
     * 6) Equals method. Implementation of the equals method is required. A
     * casting error can happen here, but we don't check it for efficiency
     * reasons.
     * @param object
     *            The object to compare.
     * @return true if this and object are equal.
     */
    public final boolean equals(final Object object) {
        L1 o = (L1) object;
        return Arrays.equals(vector, o.vector);
    }



	@Override
	public byte[] store() throws OBException, IOException {
		/*ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream o = new DataOutputStream(out);
		for(short s : vector){
			o.writeShort(s);
		}
		o.close();*/
		ByteBuffer b = ByteBuffer.allocate(ByteConstants.Short.getSize() * vector.length);
		ShortBuffer s = b.asShortBuffer();
		s.put(vector);
		return b.array();		
	}

}
