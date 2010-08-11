package net.obsearch.example.vectors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;



import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.ob.OBInt;
import net.obsearch.ob.OBLong;
import net.obsearch.ob.OBShort;
import net.obsearch.utils.bytes.ByteConversion;


public class L1Float implements OBFloat {
	
	private float[] vector;
	
	public L1Float(){
		// required by OBSearch
	}
	
	/**
	 * Construct an object from an array.
	 * @param vector
	 */
	public L1Float(float[] vector){
		this.vector = vector;
	}
	/**
	 * Parses a string with numbers separated by spaces
	 * @param data
	 */
	public L1Float(String data)throws OBException{
		String[] split = data.split("[ |,]");
		vector = new float[split.length];
		//OBAsserts.chkAssert(vector.length == 64, "Size wrong for vector: " + vector.length);
		
		int i = 0;
		for(String s : split){
			vector[i] = Float.parseFloat(s);
			i++;
		}
	}

	@Override
	public float distance(OBFloat object) throws OBException {
		L1Float other = (L1Float)object;
		int i = 0;
		float res = 0;
		OBAsserts.chkAssert(vector.length == other.vector.length, "Vector size mismatch");
		while(i < vector.length){
			res += Math.abs(vector[i] - other.vector[i]);
			i++;
		}
		OBAsserts.chkAssert(res <= Long.MAX_VALUE, "max value exceeded");
		return res; 
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		FloatBuffer s = ByteConversion.createByteBuffer(input).asFloatBuffer();
		vector = new float[input.length / ByteConstants.Float.getSize()];
		s.get(vector);		
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
        L1Float o = (L1Float) object;
        return Arrays.equals(vector, o.vector);
    }



	@Override
	public byte[] store() throws OBException, IOException {
		ByteBuffer b = ByteConversion.createByteBuffer(ByteConstants.Float.getSize() * vector.length);
		FloatBuffer s = b.asFloatBuffer();
		s.put(vector);
		return b.array();		
	}

}
