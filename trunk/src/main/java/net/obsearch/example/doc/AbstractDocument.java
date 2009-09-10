package net.obsearch.example.doc;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * A document is a multi-set of words.
 * subclasses can implement different distance functions
 * 
 * @author Arnoldo Muller
 *
 */
public abstract class AbstractDocument implements OBFloat {
	
	/**
	 * word ids
	 */
	protected int[] ids;
	/**
	 * count ids
	 */
	protected int[] counts;
	/**
	 * Name of the document
	 */
	private String name;

	public String getName(){
		return name;
	}
	
	public AbstractDocument(){
		
	}
	
	public int size(){
		return counts.length;
	}
	
	/**
	 * Create a document from a string.
	 * @param data
	 * @throws OBException 
	 */
	public AbstractDocument(String data) throws OBException{
		String[] tokens = data.split("\\t");
		name = tokens[0];
		ids = new int[tokens.length - 1];
		counts = new int[tokens.length - 1];
		int i = 1;
		int prev = Integer.MIN_VALUE;
		while(i < tokens.length){
			String[] t = tokens[i].split(",");
			int id = Integer.parseInt(t[0]);
			OBAsserts.chkAssert(prev < id, "ids are not sorted!");
			prev = id;
			int count = Integer.parseInt(t[1]);
			ids[i-1] = id;
			counts[i-1] = count;
			i++;
		}
	}
	
	

	@Override
	public void load(byte[] input) throws OBException, IOException {
		ByteBuffer buf = ByteConversion.createByteBuffer(input);
		byte[] stringBytes = new byte[buf.getInt()];
		buf.get(stringBytes);
		name = new String(stringBytes);
		int total = buf.getInt();
		int i = 0;
		ids = new int[total];
		counts = new int[total];
		while(i < total){
			ids[i] = buf.getInt();
			counts[i] = buf.getInt();
			i++;
		}
	}

	@Override
	public byte[] store() throws OBException, IOException {
		byte[] stringBytes = name.getBytes();
		int tupleSize = (ByteConstants.Int.getSize() *2) + stringBytes.length + (ByteConstants.Int.getSize() * counts.length * 2);
		ByteBuffer buf = ByteConversion.createByteBuffer(tupleSize);
		buf.putInt(stringBytes.length);
		buf.put(stringBytes);
		// write the number of elements
		buf.putInt(counts.length);
		int i = 0;
		while(i < counts.length){
			buf.putInt(ids[i]);
			buf.putInt(counts[i]);
			i++;
		}
		return buf.array();
	}
	
	public String toString(){
		return ids.length + "";
	}

}
