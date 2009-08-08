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
	private HashFunction j = new Jenkins64();
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
		//BigInteger b = new BigInteger(digest);
		//b.mod(f);
		//return b.longValue();
		//long res = Hash32.joaatAux(digest, 0);
		//res = res << 32;
		//res = res | joaat(data);
		return j.compute(digest);
	}

}
