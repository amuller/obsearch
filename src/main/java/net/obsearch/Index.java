package net.obsearch;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.exception.UndefinedPivotsException;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.OBStoreFactory;


/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * This the main contract for OBSearch indexes. The normal lifecycle of an index
 * is: 1) Insert many records 2) Perform a freeze A freeze is an operation that
 * calculates different information on the data. The idea is to "learn" how the
 * sample data is distributed to improve performance. You cannot perform
 * searches on an unfrozen index. Note that freezing the index does not mean you
 * cannot insert new data. You can! 3) Insert/Delete new records/ and search.
 * There are indexes that wrap other indexes to provide extended functionality.
 * Please consult the documentation for each index. The index assumes that for
 * any two objects a, b: if a.equals(b) then for their corresponding byte arrays
 * ba,bb ba.equals(bb) must also hold.
 * @param <O>
 *                The object that will be indexed in the database
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public interface Index < O extends OB > {

    /**
     * Inserts the given object into the index. 
     * @param object
     *                The object to be added.
     * @return {@link net.obsearch.Status#OK} if the object was inserted.
     *         {@link net.obsearch.Status#EXISTS} if the object existed in the DB.
     *         The method getId() of {@link net.obsearch.OperationStatus} will return 
     *         the id used by the object.
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @since 0.0
     */
    OperationStatus insert(O object) throws OBStorageException, OBException,
            IllegalAccessException, InstantiationException;
    
    /**
     * Return the type that is stored in the index.
     * @return
     */
    Class<O> getType();
    
    /**
     * Load the data of object # i into the given object
     * @param i
     * @param object
     * @throws OBException
     */
    void loadObject(long i, O object) throws OBException;
    
    /**
     * Inserts the given object into the index. It does not check
     * if the object to be inserted exists in the database.     
     * @param object
     *                The object to be added.
     * @return {@link net.obsearch.Status#OK} 
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @since 0.0
     */
    OperationStatus insertBulk(O object) throws OBStorageException, OBException,
            IllegalAccessException, InstantiationException;
    
    
    
    /**
     * Inserts the given object into the index. 
     * @param object
     *                The object to be added.
     * @param id The id that will be used for the object.
     * @return If {@link net.obsearch.Status#OK} or
     *         {@link net.obsearch.Status#EXISTS} then the result will hold
     *         the id of the inserted object and the operation is successful.
     *         Otherwise an exception will be thrown.
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @since 0.0
     */
    OperationStatus insert(O object, long id) throws OBStorageException, OBException,
            IllegalAccessException, InstantiationException;
    
    /**
     * Inserts the given object into the index.  It does not check
     * if the object to be inserted exists in the database.     
     * @param object
     *                The object to be added.
     * @param id The id that will be used for the object.
     * @return {@link net.obsearch.Status#OK}
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @since 0.0
     */
    OperationStatus insertBulk(O object, long id) throws OBStorageException, OBException,
            IllegalAccessException, InstantiationException;
    
    

    /**
     * Returns true if the index is frozen.
     * @return true if the index is frozen, false otherwise
     */
    boolean isFrozen();

    /**
     * Returns true if the given object exists in the database. This method was
     * created for OBSearch internal use. If you are relying on this method a
     * lot, isn't it better to use a hash table or a B-tree instead of OBSearch?
     * O.equals(...) is used to calculate the equality of two objects.
     * @param object
     *                The object that will be searched.
     * @return {@link net.obsearch.Status#EXISTS} and the object's id if
     *         the object exists in the database, otherwise
     *         {@link net.obsearch.Status#NOT_EXISTS} is returned.
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    OperationStatus exists(O object) throws OBStorageException, OBException,
            IllegalAccessException, InstantiationException;

    /**
     * Freezes the index. From this point data can be inserted, searched and
     * deleted. The index might deteriorate at some point so every once in a
     * while it is a good idea to rebuild the index. A PivotSelector must be
     * executed before calling this method.
     * @throws IOException
     *                 if the index serialization process fails
     * @throws AlreadyFrozenException
     *                 If the index was already frozen and the user attempted to
     *                 freeze it again
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     * @throws PivotsUnavailableException 
     */
    void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBStorageException, OutOfRangeException, OBException, PivotsUnavailableException;

    

    /**
     * Deletes the given object from the database.
     * @param object
     *                The object to be deleted
     * @return {@link net.obsearch.Status#OK} and the deleted object's id
     *         if the object was found and successfully deleted.
     *         {@link net.obsearch.Status#NOT_EXISTS} if the object is not
     *         in the database.
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @since 0.0
     */
    OperationStatus delete(O object) throws OBStorageException, OBException,
            IllegalAccessException, InstantiationException, NotFrozenException;

    /**
     * This method returns the object with internal id "i". Mainly used for
     * internal validation purposes, users should not have to use this method
     * directly
     * @param i
     *                The id to be retrieved
     * @return The object associated to the given id. if the index has not been
     *         frozen. was deleted successfully
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    O getObject(long i) throws IllegalIdException, IllegalAccessException,
            InstantiationException, OBException;

    /**
     * Closes the database.
     * @throws OBStorageException
     *                 If something goes wrong with the DB.
     * @throws OBException
     */
    void close() throws OBException;

    /**
     * Returns the total number of boxes this index can hold. This is an
     * optional operation, not all indexes need to support this.
     * @return The total number of boxes the index can eventually support.
     */
    long totalBoxes();

    /**
     * Returns the box where the given object is stored. This is an optional
     * operation, not all indexes need to support this.
     * @param object
     *                The object to be analyzed
     * @return The box that corresponds to object
     * @throws OBException
     *                 User generated exception
     */
    long getBox(O object) throws OBException;

    /**
     * Returns the database size.
     * @return Number of elements in the database
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBStorageException
     *                 If the underlying storage device signaled an error.
     */
    long databaseSize() throws OBStorageException;

    
    

    /**
     * This method *must* be called after de-serializing the database object and after
     * instantiating the index.
     * This method is called by the {@link #org.obsearch.ambient.Ambient}, users
     * do not need to worry about this method.
     * @param fact
     *                The storage factory that will be used to store the data.
     * @throws OBStorageException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     * @throws OBStorageException
     *                 If something goes wrong with the DB.
     * @throws IOException
     *                 if the index serialization process fails
     */
    void init(OBStoreFactory fact) throws OBStorageException,
            NotFrozenException, IllegalAccessException, InstantiationException,
            OBException, IOException;    

    /**
     * Resets all the stats counters.
     */
    void resetStats();

    /**
     * @return A human-readable list of stats relevant for this index.
     */
    Statistics getStats() throws OBStorageException;

    /**
     * Size in bytes of the object IDs used in OBSearch.
     */
    int ID_SIZE = Long.SIZE / 8;
    
    
    /**
     * Routine that helps to debug objects. Returns some
     * human-readable information regarding the given object.
     * This method is optional.
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws OBException 
     */
    String debug(O object) throws OBException, InstantiationException, IllegalAccessException;
    
    
    /**
     * If the index should check for existent objects before freeze.
     * @param preFreezeCheck Set the flag.
     */
    public void setPreFreezeCheck(boolean preFreezeCheck);
}
