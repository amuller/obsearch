package net.obsearch.storage.cuckoo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * A pointer table holds a list of memory positions (offsets) 
 * and sizes (lengths) of data allocated for the hash table
 *
 */

public class FixedPointerTable {
	
	private static int ENTRY_SIZE = ByteConstants.Long.getSize() + ByteConstants.Int.getSize();
	
	private FileChannel data;
	
	private MappedByteBuffer map;
	
	private int entries;

	/**
	 * Creates a fixed pointer table with the given channel and number
	 * of objects
	 * @param data 
	 * @param entries
	 * @throws IOException 
	 * @throws OBException 
	 */
	public FixedPointerTable(File fileName, int entries) throws IOException, OBException{
		RandomAccessFile f = new RandomAccessFile(fileName, "rw");
		boolean create = false;
		this.entries = entries;
		if(f.length() == 0){
			f.setLength(ENTRY_SIZE * entries);
			create = true;
		}
		this.data = f.getChannel();
		map = data.map(MapMode.READ_WRITE, 0, ENTRY_SIZE * entries);
		// first time, initialize everything
		if(create){
			deleteAll();
		}
	}
	private int position(int position){
		return position * ENTRY_SIZE;
	}
	/**
	 * Set the entry number id to the offset "offset" and size "size"
	 * @param id
	 * @param offset
	 * @param size
	 */
	public void set(int id, long offset, int size){
		map.position(position(id));
		map.putLong(offset);
		map.putInt(size);
	}
	
	public void set(int id, Entry e){
		set(id, e.getOffset(), e.getLength());
	}
	
	/**
	 * Number of entries
	 * @return
	 * @throws IOException 
	 * @throws OBException 
	 */
	public int size() {
		return entries;
	}
	
	/**
	 * empty the pointer table.
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void deleteAll() throws IOException, OBException{
		int i = 0;
		int size = size();
		while(i < size){
			delete(i);
			i++;
		}
	}
	
	public void close() throws IOException{
		data.close();
	}
	
	/**
	 * Get the ith entry
	 * @param i
	 * @return
	 * @throws OBException
	 * @throws IOException
	 */
	public Entry get(int id) throws OBException, IOException{
		checkSize(id);
		map.position(position(id));
		return new Entry(map.getLong(), map.getInt());
	}
	
	
	
	private void checkSize(int i) throws OBException, IOException{
		OBAsserts.chkAssert(i < size(), "Cannot exceed current array size");
	}
	
	
	
	
	/**
	 * Deletes the ith element and takes the last element 
	 * and puts it in this position!!! Changes the order
	 * of the elements!!!.
	 * @param i Object to delete.
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void delete(int i) throws OBException, IOException{
		checkSize(i);
		set(i,-1,-1);
	}
	
	
	


}
