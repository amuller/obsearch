package net.obsearch.example.vectors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;



import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBInt;
import net.obsearch.ob.OBLong;
import net.obsearch.ob.OBShort;


public class L1Long implements OBLong {
	
	private int[] vector;
	
	public L1Long(){
		// required by OBSearch
	}
	
	/**
	 * Construct an object from an array.
	 * @param vector
	 */
	public L1Long(int[] vector){
		this.vector = vector;
	}
	/**
	 * Parses a string with numbers separated by spaces
	 * @param data
	 */
	public L1Long(String data)throws OBException{
		String[] split = data.split("[ |,]");
		vector = new int[split.length];
		//OBAsserts.chkAssert(vector.length == 64, "Size wrong for vector: " + vector.length);
		
		int i = 0;
		for(String s : split){
			vector[i] = Integer.parseInt(s);
			i++;
		}
	}

	@Override
	public long distance(OBLong object) throws OBException {
		L1Long other = (L1Long)object;
		int i = 0;
		long res = 0;
		OBAsserts.chkAssert(vector.length == other.vector.length, "Vector size mismatch");
		while(i < vector.length){
			res += Math.abs(vector[i] - other.vector[i]);
			i++;
		}
		OBAsserts.chkAssert(res <= Long.MAX_VALUE, "max value exceeded");
		return (long)res; 
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		IntBuffer s = ByteBuffer.wrap(input).asIntBuffer();
		vector = new int[input.length / ByteConstants.Int.getSize()];
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
        L1Long o = (L1Long) object;
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
		ByteBuffer b = ByteBuffer.allocate(ByteConstants.Int.getSize() * vector.length);
		IntBuffer s = b.asIntBuffer();
		s.put(vector);
		return b.array();		
	}

}
