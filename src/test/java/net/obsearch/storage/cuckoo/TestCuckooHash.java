package net.obsearch.storage.cuckoo;

import static org.junit.Assert.*;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import net.obsearch.exception.OBException;
import net.obsearch.index.utils.Directory;

import org.junit.Test;


public class TestCuckooHash {
	
	private int TEST_SIZE = 1000000;
	
	private int TEST_PERF_SIZE = 100000000;
	
	private int TEST_QUERIES = 10000;
	
	@Test
	public void testAll() throws IOException, OBException, NoSuchAlgorithmException{
		File test = new File("cuckooTest");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		CuckooHash h = new CuckooHash(TEST_SIZE , test, new Jenkins64(), new Murmur64() );
		List<byte[]> vals = TestHashFunctions.createData(TEST_SIZE);
		StaticBin1D put  = new	StaticBin1D ();
		StaticBin1D get  = new	StaticBin1D ();
		long t1 = System.currentTimeMillis();
		for(byte[] k : vals){
			long time = System.currentTimeMillis();
			h.put(k, k);
			put.add(System.currentTimeMillis() - time);											
		}
		System.out.println("Creation time: " + (System.currentTimeMillis() - t1) + " msec");
		
		for(byte[] k : vals){
			long time = System.currentTimeMillis();
			byte[] v = h.get(k);
			get.add(System.currentTimeMillis() - time);
			assertTrue(Arrays.equals(k, v));
		}
		
		System.out.println(h.getStats());
		System.out.println("GET: " + get.mean() + " PUT: " + put.mean());
		h.close();
		Directory.deleteDirectory(test);
	}
	
	@Test
	public void testInsertPerf() throws IOException, OBException{
		
		File test = new File("cuckooTestPerf");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		CuckooHash h = new CuckooHash(TEST_PERF_SIZE , test, new Jenkins64(), new Murmur64()  );

		long t1 = System.currentTimeMillis();
		int i = 0;
		long time = System.currentTimeMillis();
		List<byte[]> positive = new LinkedList<byte[]>();
		StaticBin1D put = new StaticBin1D();
		while(i < TEST_PERF_SIZE){
			byte [] k = ByteArrayStorageTest.generateByteArray();
			if(positive.size() < TEST_QUERIES){
				positive.add(k);
			}
			h.put(k, k);
			CuckooHashStats s = h.getStats();
			if(i % 100000 == 0){
				long elapsed = System.currentTimeMillis() - time;
				put.add(elapsed);
				System.out.println("Loading: " + i + " " + s.getH1Inserts() + " " + s.getH2Inserts() + " depth: " + s.getBucketDepth().mean() + " std: " + s.getBucketDepth().standardDeviation() + " max: " + s.getBucketDepth().max() + " msec: " + (elapsed));
				time = System.currentTimeMillis();
			}
			i++;
		}
		StaticBin1D getNegative  = new	StaticBin1D ();
		StaticBin1D getPositive = new	StaticBin1D ();
		i = 0;
		List<byte[]> negative = TestHashFunctions.createData(10000);
		for(byte[] b : positive){
			time = System.currentTimeMillis();			
			byte[] v = h.get(b);
			getPositive.add(System.currentTimeMillis() - time);
			assertTrue(Arrays.equals(b, v));
		}
		
		for(byte[] b : negative){
			time = System.currentTimeMillis();			
			byte[] v = h.get(b);
			getNegative.add(System.currentTimeMillis() - time);			
		}
		
		System.out.println("Total creation time: " + (System.currentTimeMillis() - t1) + " msec");
		System.out.println(h.getStats());
		System.out.println("GET negative: " + getNegative.mean());
		System.out.println("Get positive: " + getPositive.mean());
		System.out.println("Time of inserts per 100000: " + put.mean());
		h.close();
		Directory.deleteDirectory(test);
	}

}
