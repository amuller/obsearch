package net.obsearch.index;

import java.util.Random;

public class VectorTestFrameworkShort extends TestFrameworkShort<OBVectorShort> {

	private int vectorDimensionality;
	private Random r;
	public VectorTestFrameworkShort(int vectorDimensionality, int dbSize, int querySize,
			IndexShort<OBVectorShort> index) {
		super(OBVectorShort.class, dbSize, querySize, index);
		this.vectorDimensionality = vectorDimensionality;
		r = new Random();
	}

	@Override
	protected OBVectorShort next() {
		short[] vector = new short[vectorDimensionality];
		int i = 0;
		while(i < vector.length){
			vector[i] = (short)r.nextInt(Short.MAX_VALUE/vectorDimensionality);
			i++;
		}
		return new OBVectorShort(vector);
	}
	
	protected void search() throws Exception{
		super.search();
		search(index, (short)(Short.MAX_VALUE/vectorDimensionality * 6) , (byte) 3);       
	}
	
	
}
