package net.obsearch.storage.cuckoo;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import net.obsearch.exception.OBException;

import org.junit.Test;


public class PointerTableTest {
	
	private static int TEST_SIZE = 1000;
	Random r = new Random();
	@Test
	public void testPointerTable() throws IOException, OBException{
		File file = new File("testPointerTable.rm");
		file.delete();
		RandomAccessFile f = new RandomAccessFile(file, "rw");
		
		PointerTable pt = new PointerTable(f.getChannel());
		
		List<Entry> t = new ArrayList(TEST_SIZE);
		int i = 0;
		while(i < TEST_SIZE){
			t.add(generate());
			i++;
		}
		
		for(Entry e : t){
			pt.add(e);			
		}
		// datasets should be the same.
		compareData(pt , t);
		
		// perform some random modifications.
		i = 0;
		while(i < 100){
			int toModify = r.nextInt(TEST_SIZE);
			Entry newEntry = generate();
			t.set(toModify, newEntry);
			pt.set(toModify, newEntry);
			compareData(pt , t);
			i++;
		}
		// after setting data things should be the same.
		compareData(pt , t);
		
		// shrinking should be also cool
		t.remove(t.size() - 1);
		pt.shrinkByOne();
		compareData(pt , t);
		
		// testing deletes	
		i = 0;
		while(i < 100){
			int toDelete = r.nextInt(t.size());
			deleteFromT(toDelete, t);
			pt.delete(toDelete);
			compareData(pt , t);
			i++;
		}
		compareData(pt , t);

		// delete first
		deleteFromT(0, t);
		pt.delete(0);
		compareData(pt , t);
		// delete last
		int last = t.size() -1;
		deleteFromT(last, t);
		pt.delete(last);
		compareData(pt , t);
		
		
		compareData(pt , t);
		
		f.close();
		assertTrue(file.delete());
	}
	
	private void deleteFromT(int toDelete, List<Entry> t){
		// get the last guy.
		Entry e = t.get(t.size() - 1);
		t.set(toDelete, e);
		t.remove(t.size() - 1);
	}
	
	private void compareData(PointerTable pt, List<Entry> t) throws OBException, IOException{
		int i  = 0;
		assertTrue( "T: " + t.size() + " pt size: " + pt.size(), t.size() == pt.size());
		while(i < t.size()){
			assertEquals("at index: " + i ,pt.get(i), t.get(i) );
			i++;
		}
	}
	
	private Entry generate(){
		return new Entry(r.nextLong(), r.nextInt());
	}

}
