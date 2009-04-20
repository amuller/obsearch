package net.obsearch.storage.cuckoo;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import net.obsearch.exception.OBException;
import net.obsearch.index.utils.Directory;

import org.junit.Test;


public class CuckooRecordManagerTest {
	
	Random r = new Random();
	static int TEST_SIZE = 1000;
	@Test
	public void testAll() throws IOException, OBException{
		
		
		File test = new File("cuckooRecordManagerTest");
		Directory.deleteDirectory(test);
		assertTrue(test.mkdirs());
		
		// create some entries by "add"
		List<CuckooEntry> data =  generateEntries(TEST_SIZE);
		CuckooRecordManager man = new CuckooRecordManager(test);
		long id = 0;
		for(CuckooEntry c : data){
			assertEquals(id, man.addEntry(c));
			id++;
		}
		// compare the records.
		compareRecords(data, man);
		
		// Try sets. 
		
		int i = 0;
		while(i < 100){
			int newObjectId = r.nextInt(TEST_SIZE);
			CuckooEntry newObject = generateEntry();
			data.set(newObjectId, newObject);
			man.putEntry(newObjectId, newObject);
			compareRecords(data, man);	
			i++;
		}
		compareRecords(data, man);
		
		// try multiple entries
		i = 0;
		while(i < 100){
			List<CuckooEntry> entries =  generateEntries(r.nextInt(200));
			data.addAll(entries);
			// put the entries at some point.
			for(CuckooEntry c : entries){
				man.putEntrySequence(i, c);
			}
			entries.add(0, man.getEntry(i));
			// compare!
			List<CuckooEntry> otherEntries = man.getEntrySequence(i);
			assertTrue(entries.equals(otherEntries));			
			i++;
		}
		compareRecords(data, man);
		
		// try deletes
		while(i < 100){
			int toDelete = r.nextInt(data.size());
			man.delete(toDelete);
			data.remove(toDelete);
			compareRecords(data, man);
			i++;
		}
		compareRecords(data, man);
	}
	
	public void compareRecords(List<CuckooEntry> data, CuckooRecordManager man) throws OBException, IOException{
		assertTrue(data.size() == man.size());
		long id = 0;
		for(CuckooEntry d : data){
			CuckooEntry other = man.getEntry(id);
			assertEquals( "At position: " + id , d, other);
			id++;
		}
	}
	
	public List<CuckooEntry> generateEntries(int size) throws OBException{
		List<CuckooEntry> result = new ArrayList<CuckooEntry>(size);
		int i = 0;
		while(i < size){
			result.add(generateEntry());
			i++;
		}
		return result;
	}
	
	public CuckooEntry generateEntry() throws OBException{
		CuckooEntry res = new CuckooEntry(ByteArrayStorageTest.generateByteArray(), ByteArrayStorageTest.generateByteArray());
		return res;
	}

}
