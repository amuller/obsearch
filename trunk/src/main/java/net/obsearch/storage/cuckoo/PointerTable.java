package net.obsearch.storage.cuckoo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * A pointer table holds a list of memory positions (offsets) 
 * and sizes (lengths) of data allocated in the pool.
 *
 */

public class PointerTable {
	
	private static int ENTRY_SIZE = ByteConstants.Long.getSize() + ByteConstants.Int.getSize();
	
	private FileChannel data;
	
	public PointerTable(FileChannel data){
		this.data = data;
	}
	
	/**
	 * Number of entries
	 * @return
	 * @throws IOException 
	 */
	public long size() throws IOException{
		return (long)data.size()/ ENTRY_SIZE;
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
	public Entry get(long i) throws OBException, IOException{		
		OBAsserts.chkAssert(i < size(), "Invalid item: " + i);
		ByteBuffer buf = ByteConversion.createByteBuffer(ENTRY_SIZE);		
		data.read(buf, (i * ENTRY_SIZE));
		buf.rewind();
		long offset = buf.getLong();
		int length = buf.getInt();
		return new Entry(offset, length);
	}
	
	public void set(long i, Entry entry) throws IOException, OBException{
		checkSize(i);
		setAux(i, entry);
	}
	
	private void checkSize(long i) throws OBException, IOException{
		OBAsserts.chkAssert(i < size(), "Cannot exceed current array size");
	}
	
	/**
	 * Add the entry to the last position
	 * @param entry
	 * @throws IOException
	 */
	public void add(Entry entry) throws IOException{
		setAux(size(), entry);	
	}
	
	private void setAux(long i, Entry entry) throws IOException{
		// TODO: optimize usage of these ByteBuffer objects.
		// we do not need to create them all the time.
		ByteBuffer buf = ByteConversion.createByteBuffer(ENTRY_SIZE);
		buf.putLong(entry.getOffset());
		buf.putInt(entry.getLength());
		buf.rewind();
		data.write(buf, (i * ENTRY_SIZE));	
	}
	
	/**
	 * Deletes the ith element and takes the last element 
	 * and puts it in this position!!! Changes the order
	 * of the elements!!!.
	 * @param i Object to delete.
	 * @throws IOException 
	 * @throws OBException 
	 */
	public void delete(long i) throws OBException, IOException{
		checkSize(i);
		// get the last element
		Entry e = get(size() - 1);
		// set the element to the one we are going to delete.
		set(i, e);
		// shrink the dataset.
		shrinkByOne();
	}
	
	/**
	 * Shrink the size of the list by one.
	 * @throws IOException 
	 */
	public void shrinkByOne() throws IOException{
		data.truncate(data.size() - ENTRY_SIZE);
	}
	


}
