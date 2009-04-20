package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import net.obsearch.exception.OBException;

/**
 * This array of CuckooEntries works like this:
 * The hashing decides that 
 * @author amuller
 *
 */
public class CuckooRecordManager {
	
	private StaticBin1D depth = new StaticBin1D();
	
	private ByteArrayStorage store;
	
	public CuckooRecordManager(File in) throws IOException{
		store = new ByteArrayStorage(in);
	}

	public void close() throws IOException{
		store.close();
	}
	
	/**
	 * Get the ith cuckoo entry, returns null if there is nothing in the store.
	 * @param i The ith entry
	 * @return the entry
	 * @throws IOException 
	 * @throws OBException 
	 */
	public CuckooEntry getEntry(long i) throws OBException, IOException{
		byte[] data = store.get(i);
		if(data == null){
			return null;
		}
		CuckooEntry res = new CuckooEntry(i);
		res.load(data);
		return res;
	}
	
	/**
	 * Return the cuckoo entry(ies) starting from the given location.
	 * if the key parameter is not null only returns the cucko entry that
	 * corresponds to it.
	 * @param location
	 * @param key
	 * @return null if there are no objects in the given position. or if the key does not match
	 * @throws IOException 
	 * @throws OBException 
	 */
	public List<CuckooEntry> getEntrySequence(long i) throws IOException, OBException{
		
		List<CuckooEntry> result = new LinkedList<CuckooEntry>();
		CuckooEntry d = getEntry(i);
		result.add(d);
		while(d.hasNext()){
			d = getEntry(d.getNext());
			result.add(d);
		}
		return result;
	}
	
	public long size() throws IOException{
		return store.size();
	}
	
	public void delete(long i) throws OBException, IOException{
		store.delete(i);
	}
	
	public void deleteSequence(long i) throws OBException, IOException{
		List<CuckooEntry> sequence =  getEntrySequence(i);
		for(CuckooEntry c : sequence){
			delete(c.getId());
		}
	}
	
	/**
	 * Put the Cuckoo entry in the given slot.
	 * @param entry
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void putEntry(long i , CuckooEntry entry) throws IOException, OBException{
		byte[] data = entry.store();
		store.put(i, data);
	}
	
	/**
	 * Add the entry at the end of the array.
	 * @param entry
	 * @return
	 * @throws IOException
	 */
	public long addEntry(CuckooEntry entry) throws IOException{
		byte[] data = entry.store();
		return store.add(data);
	}
	
	/**
	 * Put the given entry in the last of the sequences (hopefully we will do few)
	 * @param i
	 * @param entry
	 * @throws OBException 
	 * @throws IOException 
	 */
	public void putEntrySequence(long i,  CuckooEntry entry) throws IOException, OBException{
		List<CuckooEntry> sequence =  getEntrySequence(i);
		if(sequence.size() == 0){
			putEntry(i, entry);
		}else{
			// get the last guy
			CuckooEntry last = sequence.get(sequence.size() - 1);
			long newAddress = addEntry(entry);
			last.setNext(newAddress);
			// store the last guy (now it is not the last)
			putEntry(last.getId(), last);
		}
	}
	
	

}
