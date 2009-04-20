package net.obsearch.storage.cuckoo;

import java.math.BigInteger;
import java.security.MessageDigest;
/**
 * Expensive but high quality crypto hash.
 * @author amuller
 *
 */
public abstract class AbstractCryptoHash implements HashFunction{
	
	private MessageDigest m;
	private BigInteger f;
	/**
	 * Create a new crypto hash
	 * @param i i is only the 
	 * @param m
	 */
	protected AbstractCryptoHash(int i, MessageDigest m){
		
		this.m = m;
		f = BigInteger.valueOf(Long.MAX_VALUE);
	}
	
	
	
	public long compute(byte[] data){
		byte[] digest = m.digest(data);
		// java way of doing byte hashes.
		long hashCode = 0;
		//s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
		// TODO fix this.
		throw new IllegalArgumentException();
		for(byte b : digest){
			//hashCode += b* Byte.MAX_VALUE
		}
		return hashCode;
	}

}
