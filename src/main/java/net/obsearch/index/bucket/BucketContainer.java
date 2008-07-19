package net.obsearch.index.bucket;

import java.nio.ByteBuffer;
import java.util.List;

import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;


import com.sleepycat.je.DatabaseException;

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
 * A BucketContainer stores SMAP vectors of objects. It is possible to search,
 * add and remove objects stored here.
 * @param <O>
 *                OB object.
 * @param <B>
 *                The bucket that will be employed.
 * @author Arnoldo Jose Muller Molina
 */
public interface BucketContainer < O extends OB, B extends ObjectInBucket, Q > {

    /**
     * Deletes the given object from this {@link BucketContainer}.
     * @param bucket
     *                This will should match this bucket's id. Used to pass
     *                additional information such as the SMAP vector
     * @param object
     *                The object that will be deleted.
     * @return {@link net.obsearch.Status#OK} and the deleted
     *         object's id if the object was found and successfully deleted.
     *         {@link net.obsearch.Status#NOT_EXISTS} if the object
     *         is not in the database.
     */
    OperationStatus delete(B bucket, O object) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException;

    /**
     * Inserts the given object with the given bucket details to this bucket.
     * Warning: This method assumes that object does not exist in the DB. In
     * bucket, an id will be provided by the caller.
     * @param bucket
     *                This will should match this bucket's id. Used to pass
     *                additional information such as the SMAP vector.
     * @return If {@link net.obsearch.Status#OK} or
     *         {@link net.obsearch.Status#EXISTS} then the result
     *         will hold the id of the inserted object and the operation is
     *         successful.
     */
    OperationStatus insert(B bucket) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException;
    
    /**
     * Inserts the given list of buckets into this bucket.
     * @param bucket
     * @throws OBException
     * @throws DatabaseException
     * @throws IllegalIdException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    void bulkInsert(List<B> bucket) throws OBException,
    IllegalIdException, IllegalAccessException, InstantiationException;

    /**
     * Returns true if the object and its bucket definition exist in this
     * container
     * @param bucket
     *                The bucket associated to object
     * @param object
     *                The object that will be inserted
     * @return true if object exists in this container.
     * @throws OBException
     * @throws DatabaseException
     * @throws IllegalIdException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @return {@link net.obsearch.Status#EXISTS} and the object's
     *         id if the object exists in the database, otherwise
     *         {@link net.obsearch.Status#NOT_EXISTS} is returned.
     */
    OperationStatus exists(B bucket, O object) throws OBException, 
            IllegalIdException, IllegalAccessException, InstantiationException;

    /**
     * Get the byte representation of this bucket.
     * @return
     */
    ByteBuffer getBytes();

    /**
     * Searches the given object with the given searchContainer parameters. The
     * searchContainer will be updated as necessary.
     * @param query
     *                The search parameters (range, priority queue with the
     *                closest elements)
     * @param bucket
     *                The object of the given object.
     * @param object
     *                The object that will be searched.
     * @return # of distance computations executed.
     */
    long search(Q query, B bucket) throws IllegalAccessException,
            DatabaseException, OBException, InstantiationException,
            IllegalIdException;
    
    /**
     * # of objects in this container.
     * @return The # of objects in this container.
     */
    int size();
    
    /**
     * # of pivots for this container.
     * @return
     */
    int getPivots();
    
    /**
     * Sets the # of pivots for this container.
     */
    void setPivots(int pivots);

}
