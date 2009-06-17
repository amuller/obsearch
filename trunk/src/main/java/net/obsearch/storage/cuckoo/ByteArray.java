package net.obsearch.storage.cuckoo;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.util.Iterator;

import net.obsearch.exception.OBException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleLong;

/**
 * This is a dynamic byte[] array (byte[][]). You can extend it and store the
 * array at the end.
 * 
 * @author Arnoldo Jose Muller-Molina
 * 
 */
public interface ByteArray {

	/**
	 * Puts the given id and data in the database. Assumes that the position was
	 * created already in the array. So it either is empty or occupied but
	 * inside size();
	 * 
	 * @param id
	 * @param data
	 * @throws IOException
	 * @throws OBException
	 */
	public  void put(long id, byte[] data) throws OBException,
			IOException;

	/**
	 * Get the ith item.
	 * 
	 * @throws IOException
	 * @throws OBException
	 */
	public  byte[] get(long i) throws OBException, IOException;

	/**
	 * Delete the ith entry.
	 * 
	 * @param i
	 * @throws IOException
	 * @throws OBException
	 */
	public  boolean delete(long i) throws OBException, IOException;

	/**
	 * Add the given object to the end of the file.
	 * 
	 * @param data
	 * @return
	 * @throws IOException
	 * @throws OBException
	 */
	public long add(byte[] data) throws IOException, OBException;

	/**
	 * 
	 * @return The maximum size of the array.
	 * @throws IOException
	 */
	public  long size() throws IOException;

	/**
	 * Returns an iterator of the byte array.
	 * 
	 * @return
	 */
	public  CloseIterator<TupleLong> iterator();

	/**
	 * Return internal fragmentation if available.
	 * 
	 * @return
	 */
	public StaticBin1D fragmentationReport() throws IOException, OBException;
	
	/**
	 * Close the storage device
	 */
	public void close()throws IOException, OBException;
	
	
	/**
	 * Delete all data in the storage device
	 * @throws IOException 
	 */
	public void deleteAll() throws IOException;
		
	

}