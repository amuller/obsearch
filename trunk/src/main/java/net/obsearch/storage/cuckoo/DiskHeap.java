package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;

/**
 * This class considers the hard disk as main memory and provides
 * malloc and delete operations on a file. 
 * It provides a very simple space allocation technique that is prone to
 * fragmentation. 
 * @author amuller
 *
 */
public class DiskHeap {
	

	/**
	 * Where all the data is located
	 */
	private RandomAccessFile main;
	
	
	/**
	 * We keep track of released space here.
	 */
	private PointerTable holes;
	
	private ArrayList<Entry> holesCached;
	
	
	/**
	 * Create a new byte array storage in the given folder
	 * 
	 * @param in
	 * @throws FileNotFoundException
	 * @throws IOException 
	 * @throws OBException 
	 */
	public DiskHeap(File in) throws  IOException, OBException {
		try{
		RandomAccessFile mainF = new RandomAccessFile(new File(in, "main.az"),
				"rw");
		RandomAccessFile holesF = new RandomAccessFile(
				new File(in, "holes.az"), "rw");
		
		
		
		
		main = mainF;
		holes = new PointerTable(holesF.getChannel());
		// read all the holes and keep them in memory.
		
		int size = (int)holes.size();
		holesCached = new ArrayList<Entry>(size);
		int i = 0;
		while(i < size){
			holesCached.add(holes.get(i));
			i++;
		}
		}catch(FileNotFoundException e){
			throw new OBStorageException(e);
		}
		
		
		
		
	}
	
	
	
	
	/**
	 * Search a hole of size size
	 * @param size
	 * @return
	 */
	private Entry searchAndRemoveHole(int size){
		Iterator<Entry> it = holesCached.iterator();
		while(it.hasNext()){
			Entry e = it.next(); 
			if(e.getLength() == size){
				it.remove();
				return e;
			}
		}
		return null;
	}
	/**
	 * Read the given memory position.
	 * @param offset
	 * @param data
	 * @throws IOException
	 */
	public void read(long offset, byte[] data) throws IOException{
		ByteBuffer buf = ByteBuffer.wrap(data);
		main.seek(offset);
		main.readFully(data);
	}
	
	/**
	 * Store the data in memory
	 * @param data the data to store
	 * @return the address of the data.
	 * @throws IOException 
	 */
	public long store(byte[] data) throws IOException{
		
		long res;
		// get a hole or use the end of the file
		Entry hole = searchAndRemoveHole(data.length);
		if(hole != null){
			res = hole.getOffset();
			// delete the hole			
		}else{
			res = main.length(); // go to the end of the file.
		}		
		main.seek(res);
		main.write(data);
		return res;
	}
	
	/**
	 * Release the given address and # of bits.
	 * @param offset
	 * @param size
	 */
	public void release(long offset, int size){
		holesCached.add(new Entry(offset, size));
	}
	
	public void close() throws IOException{
		// save the holes		
		holes.deleteAll();
		for(Entry e : holesCached){
			holes.add(e);
		}
		
		
	}
	
	/**
	 * Empty the heap.
	 * @throws IOException
	 */
	public void deleteAll() throws IOException{
		holes.deleteAll();
		main.setLength(0);
	}

	
	
	public StaticBin1D fragmentationReport() throws IOException, OBException{
		StaticBin1D s = new StaticBin1D();
		for(Entry e : holesCached){
			s.add(e.getLength());
		}
		return s;
	}
}
