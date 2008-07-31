package net.obsearch.index.bucket;

import java.nio.ByteBuffer;

import net.obsearch.Index;
import net.obsearch.ob.OBShort;

public class BucketContainerShort<O extends OBShort> extends AbstractBucketContainerShort<O, BucketObjectShort>{

	public BucketContainerShort(Index<O> index, ByteBuffer data, int pivots) {
		super(index, data, pivots);
		
	}
	
	protected BucketObjectShort instantiateBucketObject(){
    	return new BucketObjectShort();
    }
    

}
