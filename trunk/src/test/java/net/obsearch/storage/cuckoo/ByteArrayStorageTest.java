package net.obsearch.storage.cuckoo;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.obsearch.exception.OBException;
import net.obsearch.index.utils.Directory;

import org.junit.Test;

import weka.core.Debug.Random;

/**
 * Test our secondary storage byte array class.
 * @author amuller
 *
 */
public class ByteArrayStorageTest {
	
	private static int TEST_SIZE = 100000;
	private static Random r = new Random();
	
	public static byte[] generateByteArray(){
		return String.valueOf(r.nextLong()).getBytes();
	}
	
	@Test
	public void testAll() throws IOException, OBException{
		
		File test = new File("byteArrayTest");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		
		byte[][] data = new byte[TEST_SIZE][];
		int i = 0;
		while(i < TEST_SIZE){
			data[i] = (generateByteArray());
			i++;
		}
		
		ByteArrayStorage store = new ByteArrayStorage(test);
		
		i = 0;
		long time = System.currentTimeMillis();
		while(i < TEST_SIZE){
			assertTrue(store.add(data[i]) == i);
			i++;
		}
		System.out.println("Total DB creation time: " + (System.currentTimeMillis() - time));
		// make sure they are equal.
		verifyData(data, store);
		
		// change some values and see how it goes.
		i = 0;
		while(i < 100){
			byte[] newValue =  (String.valueOf(r.nextLong()).getBytes());
			int newValueIndex = r.nextInt(TEST_SIZE);
			data[newValueIndex] = newValue;
			store.put(newValueIndex, newValue);
			verifyData(data, store);
			i++;
		}
		
		// try some random deletions
		List<Long> guysToDelete = new LinkedList<Long>();
		guysToDelete.add((long)TEST_SIZE-1); // last
		guysToDelete.add(0L); // first		
		i = 0;
		while(i < 100){
			int toDelete = r.nextInt(data.length);
			data[toDelete] = null;
			store.delete(toDelete);
			verifyData(data, store);
			i++;
		}
		
		// now that we have done these things, iterators should return exactly the same thing.
		Iterator<byte[]> it = store.iterator();
		i = 1;
		for(byte[] d : data){
			if(d == null){
				// skip
				i++;
				continue;
			}
			assertTrue("At index: " + i , i < data.length == it.hasNext());
			//assertTrue("At index: " + i ,it.hasNext());
			byte[] d2 = it.next();
			assertTrue(Arrays.equals(d, d2));
			i++;
		}
		assertTrue(! it.hasNext());
		
		System.out.println("Fragmentation:");
		System.out.println(store.fragmentationReport());
		
	}
	
	private void verifyData(byte[][] data, ByteArrayStorage store) throws IOException, OBException{
		assertTrue(data.length == store.size());
		int i = 0;
		while(i < data.length){
			assertTrue(Arrays.equals(data[i], store.get(i)));
			i++;
		}
	}

}
