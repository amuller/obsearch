package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class TestHashFunctions {
	
	private int TEST_SIZE = 5000000;
	static Random r = new Random();
	public  static List<byte[]> createData(int size){
		List<byte[]> result = new LinkedList<byte[]>();
		HashSet<Long> exists = new HashSet<Long>();
		int i = 0;
		while(i < size){
			long d = r.nextLong();
			if(exists.add(d)){
				result.add(String.valueOf(d).getBytes());
				i++;
			}
		}
		return result;
	}
	
	@Test 
	public void testHashFunctions() throws NoSuchAlgorithmException{
		List<byte[]> m = createData(TEST_SIZE);
		testHash(new Murmur64(), m);				
		testHash(new Jenkins64(), m);
		testHash(new MurmurOaaT(), m);
		testHash(new JenkinsOaaT(), m);
		testHash(new JenkinsMurmur(), m);
		testHash(new SHA1CryptoHash(), m);
		testHash(new MD5CryptoHash(), m);
	}
	
	public void testHash(HashFunction f, List<byte[]> m){
		// do it once.
		int i = 0;
		for(byte[] key : m){
			f.compute(key);
			if(i == 20000){
				break;
			}
		}
		HashSet<Long> collisions = new HashSet<Long>();
		int colls = 0;
		// measure it.
		StaticBin1D s = new StaticBin1D();
		for(byte[] key : m){
			long time = System.nanoTime();
			long d = f.compute(key);			
			s.add(System.nanoTime() - time);
			if(! collisions.add(d)){
				colls++;
			}
		}
		
		System.out.println("HASH: " + f.getClass().getName() + " time: " + s.mean() + " colls: " + colls);
	}

}
