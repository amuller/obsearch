package net.obsearch.storage.cuckoo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class SHA1CryptoHash extends AbstractCryptoHash {
	public SHA1CryptoHash() throws NoSuchAlgorithmException{
		super(Security.addProvider(new BouncyCastleProvider()), MessageDigest.getInstance("MD5"));		
	}
}
