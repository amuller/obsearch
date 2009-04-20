package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class TestingDigestMessages {

	public static void main(String[] args) throws UnsupportedEncodingException, NoSuchAlgorithmException, NoSuchProviderException {
		Security.addProvider(new BouncyCastleProvider());

		byte[] key = "TEXTMINE".getBytes();
		System.out.println("Byte size:" + key.length);
		//
		// Get a message digest object using the MD2 algorithm BOUNCY
		// MessageDigest messageDigest = MessageDigest.getInstance("MD2");
		//
		timeDigest(MessageDigest.getInstance("MD2", "BC"), key);
		// Get a message digest object using the MD5 algorithm
		timeDigest(MessageDigest.getInstance("MD5"), key);
		
		timeDigest(MessageDigest.getInstance("MD5", "BC"),key);
		
		// Get a message digest object using the SHA-1 algorithm
		timeDigest(MessageDigest.getInstance("SHA-1", "BC"), key);
		
		timeDigest(MessageDigest.getInstance("Whirlpool", "BC"), key);
		
		timeDigest(MessageDigest.getInstance("SHA-1", "BC"), key);
		
		timeDigest(MessageDigest.getInstance("RipeMD128", "BC"), key);
		
		timeDigest(MessageDigest.getInstance("GOST", "BC"), key);
		
		timeDigest(MessageDigest.getInstance("Tiger", "BC"), key);
		//
		// Get a message digest object using the SHA-256 algorithm
		// MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
		//
		// Get a message digest object using the SHA-384 algorithm
		// MessageDigest messageDigest = MessageDigest.getInstance("SHA-384");
		//
		// Get a message digest object using the SHA-512 algorithm
		// MessageDigest messageDigest = MessageDigest.getInstance("SHA-512");
		//
		// Print out the provider used
		//System.out.println("\n" + messageDigest.getProvider().getInfo());
		//
		// Calculate the digest and print it out
		
		/*
		 * 


ALGO: MD5 SUN mean: 463.6150333333333   <<:)>>
ALGO: MD5 BC mean: 889.7946
ALGO: SHA-1 BC mean: 691.343
ALGO: Whirlpool BC mean: 4889.4527333333335
ALGO: SHA-1 BC mean: 872.8591333333334
ALGO: RIPEMD128 BC mean: 596.8223333333333
ALGO: GOST3411 BC mean: 26736.434
ALGO: Tiger BC mean: 504.3885333333333 <<:)>>


		 */

	}
	public static void timeDigest(MessageDigest m, byte[] key){
		int i = 0;
		StaticBin1D s = new StaticBin1D();
		StaticBin1D bigInt = new StaticBin1D();
		while(i < 20000){
			m.digest(key);
			i++;
		}
		i = 0;
		BigInteger f = BigInteger.valueOf(20000000);
		while(i < 30000){
			long time = System.nanoTime();
			byte[] data = m.digest(key);
			s.add(System.nanoTime() - time);
			time = System.nanoTime();
			BigInteger value = new BigInteger(data);
			long res = value.mod(f).longValue();
			bigInt.add(System.nanoTime() - time);
			i++;
		}
		System.out.println("ALGO: " + m.getAlgorithm() + " " + m.getProvider().getName() + " mean: " +s.mean() + " bigint: " + bigInt.mean());
		//System.out.println(s);
	}
}
