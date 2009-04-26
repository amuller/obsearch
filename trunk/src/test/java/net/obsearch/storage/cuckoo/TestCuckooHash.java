package net.obsearch.storage.cuckoo;

import static org.junit.Assert.*;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.obsearch.exception.OBException;
import net.obsearch.index.utils.Directory;
import net.obsearch.storage.TupleBytes;

import org.junit.Test;

public class TestCuckooHash {

	protected int TEST_SIZE = 100000;

	private int TEST_PERF_SIZE = 1000000;

	private int TEST_QUERIES = 10000;

	protected HardDiskHash createHash(File location) throws IOException,
			OBException {
		return new CuckooHash(TEST_SIZE, location, new Murmur64(),
				new Jenkins64());
	}

	@Test
	public void testAll() throws IOException, OBException,
			NoSuchAlgorithmException {
		File test = new File("cuckooTest");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		HardDiskHash h = createHash(test);
		LinkedList<byte[]> vals = TestHashFunctions.createData(TEST_SIZE);
		StaticBin1D put = new StaticBin1D();
		StaticBin1D get = new StaticBin1D();
		long t1 = System.currentTimeMillis();
		for (byte[] k : vals) {
			long time = System.currentTimeMillis();
			h.put(k, k);
			put.add(System.currentTimeMillis() - time);
		}
		assertTrue(" Hash size: " + h.size(), TEST_SIZE == h.size());

		// check the iterators
		Iterator<TupleBytes> it = h.iterator();
		long count = 0;
		LinkedList<byte[]> valsClone = (LinkedList<byte[]>) vals.clone();
		while (it.hasNext()) {
			TupleBytes t = it.next();
			assertTrue(verifyExists(t.getKey(), valsClone));
			assertTrue(Arrays.equals(t.getKey(), t.getValue()));
			count++;
			if (count % 10000 == 0) {
				System.out.println("Doing: " + count);
			}
		}
		assertTrue(count == h.size());
		assertTrue(valsClone.size() == 0);

		// try some repeated values
		byte[] data = "my string :)".getBytes();
		h.put(data, data);
		assertTrue(Arrays.equals(data, h.get(data)));

		// re-write the same bucket
		byte[] data2 = "my string :) feliz".getBytes();
		h.put(data, data2);
		assertTrue(Arrays.equals(data2, h.get(data)));

		// re-write the same bucket
		byte[] data3 = "my string :) feliz la la la".getBytes();
		h.put(data, data3);
		assertTrue(Arrays.equals(data3, h.get(data)));

		System.out.println("Creation time: "
				+ (System.currentTimeMillis() - t1) + " msec");

		for (byte[] k : vals) {
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

	/**
	 * Verify that key exists in
	 * 
	 * @param key
	 */
	private boolean verifyExists(byte[] key, List<byte[]> data) {
		Iterator<byte[]> it = data.iterator();
		while (it.hasNext()) {
			byte[] k = it.next();
			if (Arrays.equals(k, key)) {
				it.remove();
				return true;
			}
		}		
		return false;
	}

	@Test
	public void testInsertPerf() throws IOException, OBException {

		File test = new File("cuckooTestPerf");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		HardDiskHash h = createHash(test);

		long t1 = System.currentTimeMillis();
		int i = 0;
		long time = System.currentTimeMillis();
		List<byte[]> positive = new LinkedList<byte[]>();
		StaticBin1D put = new StaticBin1D();
		while (i < TEST_PERF_SIZE) {
			byte[] k = ByteArrayStorageTest.generalArray();
			if (positive.size() < TEST_QUERIES) {
				positive.add(k);
			}
			h.put(k, k);
			CuckooHashStats s = h.getStats();
			if (i % 100000 == 0) {
				long elapsed = System.currentTimeMillis() - time;
				put.add(elapsed);
				System.out.println("Loading: " + i + " " + s.getH1Inserts()
						+ " " + s.getH2Inserts() + " depth: "
						+ s.getBucketDepth().mean() + " std: "
						+ s.getBucketDepth().standardDeviation() + " max: "
						+ s.getBucketDepth().max() + " msec: " + (elapsed));
				time = System.currentTimeMillis();
			}
			i++;
		}
		StaticBin1D getNegative = new StaticBin1D();
		StaticBin1D getPositive = new StaticBin1D();
		i = 0;
		List<byte[]> negative = TestHashFunctions.createData(10000);
		for (byte[] b : positive) {
			time = System.currentTimeMillis();
			byte[] v = h.get(b);
			getPositive.add(System.currentTimeMillis() - time);
			assertTrue(Arrays.equals(b, v));
		}

		for (byte[] b : negative) {
			time = System.currentTimeMillis();
			byte[] v = h.get(b);
			getNegative.add(System.currentTimeMillis() - time);
		}

		System.out.println("Total creation time: "
				+ (System.currentTimeMillis() - t1) + " msec");
		System.out.println(h.getStats());
		System.out.println("GET negative: " + getNegative.mean());
		System.out.println("Get positive: " + getPositive.mean());
		System.out.println("Time of inserts per 100000: " + put.mean());
		h.close();
		Directory.deleteDirectory(test);
	}

}
