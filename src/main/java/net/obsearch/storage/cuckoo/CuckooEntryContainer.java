package net.obsearch.storage.cuckoo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.obsearch.Storable;
import net.obsearch.exception.OBException;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * holds and serializes CuckooEntryCompact elements
 * @author amuller
 *
 */
public class CuckooEntryContainer implements Storable{
	
	
	private List<CuckooEntryCompact> entries;
	
	public CuckooEntryContainer(){
		entries = new ArrayList<CuckooEntryCompact>();
	}
	
	
	public void add(CuckooEntryCompact entry){
		entries.add(entry);
	}


	@Override
	public void load(byte[] input) throws OBException, IOException {
		// load the entries
		ByteBuffer buf = ByteConversion.createByteBuffer(input);
		while(buf.position() < buf.capacity()){
			CuckooEntryCompact c = new CuckooEntryCompact();
			c.load(buf);
			entries.add(c);
		}
		
	}
	
	public int size(){
		return entries.size();
	}
	
	/**
	 * Search the given key in the bucket, if it is not found, returns null
	 * @param key
	 * @return null if the key does not exist
	 */
	public CuckooEntryCompact search(byte[] key){
		for(CuckooEntryCompact c : entries){
			if(Arrays.equals(c.getKey(), key) ){
				return c;
			}
		}
		return null;
	}
	
	/**
	 * Delete the given key
	 * @param key
	 * @return
	 */
	public boolean delete(byte[] key){
		for(CuckooEntryCompact c : entries){
			if(Arrays.equals(c.getKey(),key) ){
				entries.remove(c);
				return true;
			}
		}
		return false;
	}


	@Override
	public byte[] store() throws OBException, IOException {
		int size = 0;
		for(CuckooEntryCompact c : entries){
			size += c.serializedSize();
		}
		byte[] res = new byte[size];
		ByteBuffer buf = ByteConversion.createByteBuffer(res);
		for(CuckooEntryCompact c : entries){
			c.store(buf);
		}
		assert buf.position() == buf.capacity();
		return res;
	}
	
	
	public Iterator<TupleBytes> iterator(){
		return (Iterator)entries.iterator();
	}
	
}
