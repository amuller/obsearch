package org.ajmm.obsearch.storage;

import hep.aida.bin.StaticBin1D;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.ajmm.obsearch.OperationStatus;
import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.exception.OBStorageException;

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
 * @author Arnoldo Jose Muller Molina
 */

public interface OBStore<T extends Tuple> {

    /**
     * Get the name of this storage system.
     * @return the name of this storage system.
     */
    String getName();

    /**
     * Returns the associated value for the given key. If the underlying storage
     * system can hold multiple keys, then an IllegalArgumentException is
     * thrown.
     * @param key
     *                The key that will be searched.
     * @return the associated value for the given key or null if the key could
     *         not be found.
     * @throws IllegalArgumentException
     *                 If the underlying storage system can hold multiple keys (
     *                 {@link #allowsDuplicatedData()} == true).
     * @throws OBStorageException
     *                 If an exception occurs at the underlying storage system.
     *                 You can query the exception to see more details regarding
     *                 the nature of the error.
     */
    ByteBuffer getValue(byte[] key) throws IllegalArgumentException,
            OBStorageException;

    /**
     * Inserts the key value pair. If the key existed, it will be overwritten.
     * @param key
     *                Key to insert
     * @param value
     *                The value that the key will hold after this operation
     *                completes.
     * @throws OBStorageException
     *                 If an exception occurs at the underlying storage system.
     *                 You can query the exception to see more details regarding
     *                 the nature of the error.
     * @return {@link org.ajmm.obsearch.Status#OK} the record was inserted/updated successfully.
     *               {@link org.ajmm.obsearch.Status#ERROR} if the record could not be updated.
     */
    OperationStatus put(byte[] key, ByteBuffer value) throws OBStorageException;

   

    /**
     * Deletes the given key and its corresponding value from the database.
     * @param key
     *                The key that will be deleted.
     * @throws OBStorageException
     *                 If an exception occurs at the underlying storage system.
     *                 You can query the exception to see more details regarding
     *                 the nature of the error.
     * @return {@link org.ajmm.obsearch.Status#OK} if the key was found,
     *         otherwise, {@link org.ajmm.obsearch.Status#NOT_EXISTS}.
     */
    OperationStatus delete(byte[] key) throws OBStorageException;

    /**
     * Closes the storage system.
     * @throws OBStorageException
     *                 If an exception occurs at the underlying storage system.
     *                 You can query the exception to see more details regarding
     *                 the nature of the error.
     */
    void close() throws OBStorageException;

    /**
     * Deletes all the items in the storage system. Use with care!
     * @throws OBStorageException
     *                 If an exception occurs at the underlying storage system.
     *                 You can query the exception to see more details regarding
     *                 the nature of the error.
     */
    void deleteAll() throws OBStorageException;

    /**
     * Returns the number of elements in the database. 
     * @return The number of elements in the database. 
     * @throws OBStorageException
     *                 If an exception occurs at the underlying storage system.
     *                 You can query the exception to see more details regarding
     *                 the nature of the error.
     */
    long size() throws OBStorageException;
    
    // TODO: put all the ids in longs and not in ints.
    /**
     * Returns the next id from the database (incrementing sequences). 
     * @return The next id that can be inserted. 
     */
    long nextId() throws OBStorageException;
    
    /**
     * Returns the read stats, it contains the avg # of bytes read
     *  the std deviation and also the number of reads.
     * @return
     */
    StaticBin1D getReadStats();
    
    /**
     * Sets the stats object to the given stats.
     * If null, then we stop storing the stats info.
     */
    void setReadStats(StaticBin1D stats);
    
    
    

    /**
     * Process all the elements in the DB. Useful for debugging.
     * @return An iterator that goes through all the data in the DB.
     * @throws OBStorageException
     */
    CloseIterator<T> processAll()throws OBStorageException;
    
    // TODO: For File mappings we might need to create a function that allows
    //            the user to expand the size of the buffer by some %. 
    //            We don't need this right now but the current architecture will support this.
}
