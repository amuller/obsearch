package net.obsearch.storage.cuckoo;

import static org.junit.Assert.*;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.obsearch.exception.OBException;
import net.obsearch.index.utils.Directory;
import net.obsearch.storage.TupleLong;

import org.junit.Test;

import weka.core.Debug.Random;

/**
 * Test our secondary storage byte array class.
 * @author amuller
 *
 */
public class ByteArrayStorageTest {
	
	protected static int TEST_SIZE = 100000;
	private static Random r = new Random();
	
	public static byte[] generalArray(){
		byte[] res = new byte[1 + r.nextInt(20)];
		r.nextBytes(res);
		return res;
	}
	
	public byte[] generateByteArray(){
		return String.valueOf(r.nextLong()).getBytes();
	}
	
	public byte[][] generateData(){
		byte[][] data = new byte[TEST_SIZE][];
		int i = 0;
		while(i < TEST_SIZE){
			data[i] = (generateByteArray());
			i++;
		}
		return data;
	}
	
	public ByteArray createStorage(File file) throws FileNotFoundException, OBException{
		return new ByteArrayFlex(file);
	}
	
	@Test
	public void testAll() throws IOException, OBException{
		
		File test = new File("byteArrayTest");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		
		byte[][] data = generateData();
		
		ByteArray store = createStorage(test);
		
		int i = 0;
		long time = System.currentTimeMillis();
		while(i < TEST_SIZE){
			assertTrue(store.add(data[i]) == i);
			i++;
		}
		
		
		
		System.out.println("Total DB creation time: " + (System.currentTimeMillis() - time));
		
		assertTrue(store.size() == TEST_SIZE);
		// let's do an interation
		Iterator<TupleLong> it =  store.iterator();
		i = 0;
		int cx = 1;
		while(it.hasNext()){
			TupleLong t = it.next();
			assertTrue(Arrays.equals(data[i], t.getValue()));
			assertTrue(Arrays.equals(data[i], t.getValue()));
			assertTrue(t.getKey() == i);
			i++;
			cx++;
		}
		assertTrue("i:  " + i, i == TEST_SIZE );
		
		// make sure they are equal.
		verifyData(data, store);
		
		// change some values and see how it goes.
		i = 0;
		while(i < 100){
			byte[] newValue =  generateByteArray();
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
		it = store.iterator();
		i = 0;
		cx = 0;
		StaticBin1D nextStats = new StaticBin1D();
		for(byte[] d : data){
			if(d == null){
				// skip
				cx++;
				i++;
				continue;
			}
			assertTrue("At index: " + i , i < data.length == it.hasNext());
			//assertTrue("At index: " + i ,it.hasNext());
			time = System.currentTimeMillis();
			TupleLong t = it.next();
			nextStats.add(System.currentTimeMillis() - time);
			byte[] d2 = t.getValue();
			assertTrue(Arrays.equals(d, d2));
			assertTrue(cx == t.getKey());
			i++;
			cx++;
		}
		assertTrue(! it.hasNext());
		
		System.out.println("Fragmentation:");
		System.out.println(store.fragmentationReport());
		
		System.out.println("Time per next:" + nextStats.mean());
		
	}
	
	private void verifyData(byte[][] data, ByteArray store) throws IOException, OBException{
		assertTrue(data.length == store.size());
		int i = 0;
		while(i < data.length){
			assertTrue(Arrays.equals(data[i], store.get(i)));
			i++;
		}
	}

}
