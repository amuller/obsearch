package net.obsearch.storage.cuckoo;

public interface HashFunction {
	
	
	/**
	 * Compute the hash value of the given data stream 
	 * @param data the data that will be analyzed
	 * @return hash code for the given data.
	 */
	long compute(byte[] data); 
	
	

}
