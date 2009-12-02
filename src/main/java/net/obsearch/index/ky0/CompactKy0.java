package net.obsearch.index.ky0;

import java.nio.ByteBuffer;
import java.util.Arrays;

import net.obsearch.constants.ByteConstants;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * Compact distance permutation
 * @author Arnoldo Jose Muller Molina
 *
 */
public class CompactKy0 {
	
	private int[] invertedPermutation;
	private int height;
	
	public CompactKy0(int height, int[] value) {
		super();
		this.invertedPermutation = value;
		this.height = height;
	}
	
	public int[] getValue(){
		return invertedPermutation;
	}

	@Override
	public boolean equals(Object obj) {
		CompactKy0 p = (CompactKy0)obj;
		return Arrays.equals(p.invertedPermutation, invertedPermutation);
	}
	
	

	public int[] getInvertedPermutation() {
		return invertedPermutation;
	}

	public void setInvertedPermutation(int[] invertedPermutation) {
		this.invertedPermutation = invertedPermutation;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(invertedPermutation);
	}
	
	public byte[] getAddress(){
		ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Int.getSize() * (invertedPermutation.length + 1));
		buf.putInt(height);
		for(int v : invertedPermutation){
			buf.putInt(v);
		}
		return buf.array();
	}
	
	
}
