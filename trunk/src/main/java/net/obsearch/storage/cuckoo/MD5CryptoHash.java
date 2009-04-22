package net.obsearch.storage.cuckoo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class MD5CryptoHash extends AbstractCryptoHash {
	
	public MD5CryptoHash() throws NoSuchAlgorithmException{
		super(1, MessageDigest.getInstance("MD5"));		
	}

}
