package org.ajmm.obsearch;

import java.util.Iterator;


import org.ajmm.obsearch.exception.OBException;

import com.sleepycat.je.DatabaseException;

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
 * A SynchronizableIndex can be used to perform syncrhonizations with other
 * indexes. We use timestamps as the base for this process. A boxed index is an
 * index that divides the data into boxes. In this way boxes can be distributed
 * among different computers. For n boxes, the box id is in the range [0 , n-1].
 * A SynchronizableIndex was designed to be used by other indexes within OB, you
 * do not usually need to use this kind of index.
 * @param <O>
 *            The object that will be indexed
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public interface SynchronizableIndex < O extends OB > extends Index < O > {

    /**
     * @return The total # of boxes this index can potentially hold.
     */
    int totalBoxes();

    /**
     * @return A list of the currently held boxes for this index.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    int[] currentBoxes() throws DatabaseException, OBException;

    /**
     * Returns the most recent insert /delete date for the given box. The
     * resulting long is actually a date as returned by
     * System.currentTimeMillis() Returns -1 if no data is found for the given
     * box
     * @param box
     *            The box that will be queried.
     * @return Latest inserted time in System.currentTimeMillis() format.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    long latestModification(int box) throws DatabaseException, OBException;

    /**
     * Returns the # of objects per box.
     * @param box
     *            The box that will be queried.
     * @return Number of objects stored in the given box.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    int elementsPerBox(int box) throws DatabaseException, OBException;

    /**
     * Returns an iterator with all the inserted or deleted elements newer than
     * the given date.
     * @param date
     *            Date in the format returned by System.currentTimeMillis()
     * @param box
     *            The box # that will be searched
     * @return Iterator
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    Iterator < TimeStampResult < O > > elementsNewerThan(int box, long date)
            throws DatabaseException, OBException;

    /**
     * @return The underlying index.
     */
    Index < O > getIndex();

    /**
     * Inserts the given object into the index. Forces the object to have the
     * given timestamp. This method is intended to be used internally by
     * OBSearch.
     * @param object
     *            The object to be added
     * @param time
     *            Timestamp to be used.
     * @return If {@link org.ajmm.obsearch.Result#OK} or
     *         {@link org.ajmm.obsearch.Result#EXISTS} then the result will hold
     *         the id of the inserted object and the operation is successful.
     *         Otherwise an exception will be thrown.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @since 0.0
     */
    Result insert(O object, long time) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException;

    /**
     * Deletes the given object into the index. Forces the object to be deleted
     * in the given timestamp. This method is intended to be used internally by
     * OBSearch.
     * @param object
     *            Object to insert
     * @param time
     *            The time where it should be inserted
     * @return {@link org.ajmm.obsearch.Result#OK} and the deleted object's id
     *         if the object was found and successfully deleted.
     *         {@link org.ajmm.obsearch.Result#NOT_EXISTS} if the object is not
     *         in the database.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    Result delete(O object, long time) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException;
}
