package net.obsearch.storage;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * OBStore abstracts a generic storage system. The purpose of this class is to
 * allow OBSearch to run on top of different storage systems (distributed,
 * local, file system based, etc). The keys can be sorted, and range queries are
 * possible. The base interface only allows operations on keys of arrays of
 * bytes. Subclasses of this interface will provide specialized methods for
 * Java's primitive types.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public interface OBStore<T extends Tuple> {

	/**
	 * Get the name of this storage system.
	 * 
	 * @return the name of this storage system.
	 */
	String getName();

	/**
	 * Returns the associated value for the given key. If the underlying storage
	 * system can hold multiple keys, then an IllegalArgumentException is
	 * thrown.
	 * 
	 * @param key
	 *            The key that will be searched.
	 * @return the associated value for the given key or null if the key could
	 *         not be found.
	 * @throws IllegalArgumentException
	 *             If the underlying storage system can hold multiple keys (
	 *             {@link #allowsDuplicatedData()} == true).
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 * @throws OBException 
	 * @throws IOException 
	 */
	byte[] getValue(byte[] key) throws IllegalArgumentException,
			OBStorageException, OBException;

	/**
	 * Inserts the key value pair. If the key existed, it will be overwritten.
	 * 
	 * @param key
	 *            Key to insert
	 * @param value
	 *            The value that the key will hold after this operation
	 *            completes.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 * @return {@link net.obsearch.Status#OK} the record was inserted/updated
	 *         successfully. {@link net.obsearch.Status#ERROR} if the record
	 *         could not be updated.
	 * @throws OBException 
	 */
	OperationStatus put(byte[] key, byte[] value) throws OBStorageException, OBException;
	
	
	
	
	
	

	/**
	 * Deletes the given key and its corresponding value from the database.
	 * If the storage contains duplicates, then all the elements related
	 * to the key are removed.
	 * @param key
	 *            The key that will be deleted.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 * @return {@link net.obsearch.Status#OK} if the key was found, otherwise,
	 *         {@link net.obsearch.Status#NOT_EXISTS}.
	 * @throws OBException 
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws OBException 
	 * @throws OutOfRangeException 
	 */
	OperationStatus delete(byte[] key) throws OBStorageException, IOException, OBException;

	/**
	 * Closes the storage system.
	 * 
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 * @throws OBException 
	 * @throws OBException 
	 */
	void close() throws OBStorageException;

	/**
	 * Deletes all the items in the storage system. Use with care!
	 * 
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws OBException 
	 * @throws OutOfRangeException 
	 */
	void deleteAll() throws OBStorageException;

	/**
	 * Returns the number of elements in the database.
	 * 
	 * @return The number of elements in the database.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 * @throws OBException 
	 */
	long size() throws OBStorageException;

	// TODO: put all the ids in longs and not in ints.
	/**
	 * Returns the next id from the database (incrementing sequences).
	 * 
	 * @return The next id that can be inserted.
	 */
	long nextId() throws OBStorageException;

	/**
	 * Returns the read stats, it contains the avg # of bytes read the std
	 * deviation and also the number of reads.
	 * 
	 * @return
	 */
	StaticBin1D getReadStats();

	/**
	 * Sets the stats object to the given stats. If null, then we stop storing
	 * the stats info.
	 */
	void setReadStats(StaticBin1D stats);
	
	
	/**
	 * Return the stats of this object (to be printed for the user)
	 * @return
	 */
	Object getStats() throws  OBException;

	/**
	 * Process the given range of items (from low to high), including low and
	 * high. The TupleProcessor's process method will be called for each value
	 * found within the range.
	 * 
	 * @param low lowest key value to return.
	 * @param high highest key value to return.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 */
	CloseIterator<TupleBytes> processRange(byte[] low, byte[] high)
			throws OBStorageException;
	
	
	/**
	 * Process the given range of items (from low to high), including low and
	 * high. The TupleProcessor's process method will be called for each value
	 * found within the range. No duplicate values will be returned.
	 * 
	 * @param low lowest key value to return.
	 * @param high highest key value to return.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 */
	CloseIterator<TupleBytes> processRangeNoDup(byte[] low, byte[] high)
			throws OBStorageException;

	/**
	 * Process the given range of items (from high to low), including low and
	 * high. The TupleProcessor's process method will be called for each value
	 * found within the range.
	 * 
	 * @param low lowest key value to return.
	 * @param high highest key value to return.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 */
	CloseIterator<TupleBytes> processRangeReverse(byte[] low, byte[] high)
			throws OBStorageException;
	
	/**
	 * Process the given range of items (from high to low), including low and
	 * high. The TupleProcessor's process method will be called for each value
	 * found within the range. No duplicate values will be returned.
	 * 
	 * @param low lowest key value to return.
	 * @param high highest key value to return.
	 * @throws OBStorageException
	 *             If an exception occurs at the underlying storage system. You
	 *             can query the exception to see more details regarding the
	 *             nature of the error.
	 */
	CloseIterator<TupleBytes> processRangeReverseNoDup(byte[] low, byte[] high)
			throws OBStorageException;

	/**
	 * Process all the elements in the DB. Useful for debugging.
	 * 
	 * @return An iterator that goes through all the data in the DB.
	 * @throws OBStorageException
	 * @throws OBException 
	 */
	CloseIterator<T> processAll() throws OBStorageException, OBException;
	
	

	// TODO: For File mappings we might need to create a function that allows
	// the user to expand the size of the buffer by some %.
	// We don't need this right now but the current architecture will support
	// this.
	/**
	 * Return the factory associated to this storage device.
	 */
	OBStoreFactory getFactory();
	
	
	/**
	 * Transform Bytes in a format that can be used by the 
	 * underlying index.
	 * @param in Input byte array
	 * @return transformed bytes ready to be sorted.`
	 */
	byte[] prepareBytes(byte[] in);
	
	
	/**
	 * Somehow optimizes the underlying storage representation.
	 * @throws OBStorageException
	 */
	void optimize() throws OBStorageException;
	
	
}
