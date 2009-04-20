package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class TestHashFunctions {
	
	private int TEST_SIZE = 20000;
	
	private List<byte[]> createData(){
		List<byte[]> result = new LinkedList<byte[]>();
		int i = 0;
		while(i < TEST_SIZE){
			result.add(ByteArrayStorageTest.generateByteArray());
			i++;
		}
		return result;
	}
	
	@Test 
	public void testHashFunctions() throws NoSuchAlgorithmException{
		List<byte[]> m = createData();
		
		testHash(new MurmurOaaT(), m);
		testHash(new JenkinsOaaT(), m);
		testHash(new JenkinsMurmur(), m);
		testHash(new SHA1CryptoHash(), m);
		testHash(new MD5CryptoHash(), m);
	}
	
	public void testHash(HashFunction f, List<byte[]> m){
		// do it once.
		for(byte[] key : m){
			f.compute(key);
		}
		
		// measure it.
		StaticBin1D s = new StaticBin1D();
		for(byte[] key : m){
			long time = System.nanoTime();
			f.compute(key);
			s.add(System.nanoTime() - time);
		}
		
		System.out.println("HASH: " + f.getClass().getName() + " time: " + s.mean());
	}

}
