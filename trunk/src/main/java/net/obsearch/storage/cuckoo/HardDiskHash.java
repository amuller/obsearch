package net.obsearch.storage.cuckoo;

import java.io.IOException;

import net.obsearch.exception.OBException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;

public interface HardDiskHash {

	public abstract CuckooHashStats getStats() throws IOException, OBException;

	/**
	 * Add put a key in the hash table.
	 * @param key
	 * @param value
	 * @throws IOException 
	 * @throws OBException 
	 */
	public abstract void put(byte[] key, byte[] value) throws IOException,
			OBException;

	public abstract byte[] get(byte[] key) throws IOException, OBException;

	/**
	 * Delete the given key.
	 * @param key
	 * @return
	 * @throws OBException 
	 * @throws IOException 
	 */
	public abstract boolean delete(byte[] key) throws IOException, OBException;

	public abstract void deleteAll() throws IOException;

	public abstract long size() throws IOException;

	public abstract void close() throws IOException;

	/**
	 * Iterator of all the keys and values of the hash table.
	 * @return
	 * @throws IOException 
	 * @throws OBException 
	 */
	public abstract CloseIterator<TupleBytes> iterator() throws OBException, IOException;

}