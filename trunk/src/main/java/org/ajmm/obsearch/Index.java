package org.ajmm.obsearch;

import java.io.IOException;

import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.IllegalKException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pivotselection.PivotSelector;

import com.sleepycat.je.DatabaseException;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/**
 * An Index stores objects based on a distance function in a hopefully efficient
 * way Our indexes first load a bunch of data, perform some calculations and
 * then they have to be frozen. We calculate different things such as pivots,
 * extended pyramid technique values and p+tree clustering detection. An index
 * can be frozen only once. Please make sure you add a bunch of data into it
 * before freezing it. Note that after freezing it, you can continue adding
 * data. :) 
 * In the future we will offer a "rebuild" method that optimizes the database
 * in background. This is not yet a priority
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */
public interface Index<O extends OB> {
   
    // TODO: Remove all the *newInstance() methods as they use reflection and this is very slow
    //             
    /**
     * Searches the Index and returns OBResult (ID, OB and distance) elements
     * that are closer to "object". The closest element is at the beginning of
     * the list and the farthest elements is at the end of the list This
     * condition must hold result.length == k
     * 
     * @param object
     *            The object that has to be searched
     * @param k
     *            The maximum number of objects to be returned
     * @param r
     *            The range to be used
     * @param result
     *            A priority queue that will hold the result
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @since 0.0
     */
    // TODO: Evaluate if result should be a priority queue instead of an array
  //  void searchOB(O object, D r, OBPriorityQueue<O, D> result)
   //         throws IllegalKException, NotFrozenException, DatabaseException,
   //         InstantiationException, IllegalIdException, IllegalAccessException, OutOfRangeException, OBException;

    /**
     * Inserts the given object into the index with the given ID If the given ID
     * already exists, the exception IllegalIDException is thrown.
     * 
     * @param object
     *            The object to be added
     * @param id
     *            Identification number of the given object. This number must be
     *            responsibly generated by someone
     * @return 0 if the object already existed or 1 if the object was inserted
     * @throws IllegalIdException
     *             if the given ID already exists or if isFrozen() = false and
     *             the ID's did not come in sequential order
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @since 0.0
     */
    // TODO: make sure that the community is ok with
    // storing 2,147,483,647 objects
    byte insert(O object, int id) throws IllegalIdException, DatabaseException,
            OBException ,  IllegalAccessException, InstantiationException;

    /**
     * Returns true if the index is frozen.
     * 
     * @return true if the index is frozen, false otherwise
     */
    boolean isFrozen();

    /**
     * Freezes the index. From this point data can be inserted, searched and
     * deleted The index might deteriorate at some point so every once in a
     * while it is a good idea to rebuild de index
     * 
     * @param pivotSelector
     *            The pivot selector to be used
     * @throws IOException
     *             if the serialization process fails
     * @throws AlreadyFrozenException
     *             If the index was already frozen and the user attempted to
     *             freeze it again
     */
    void freeze(PivotSelector pivotSelector) throws IOException,
            AlreadyFrozenException, IllegalIdException, IllegalAccessException,
            InstantiationException, DatabaseException, OutOfRangeException, OBException;

    /**
     * Deletes the given object form the database.
     * 
     * @param object
     *            The object to be deleted
     * @return 0 if the object was not found in the database or 1 if it
     * @throws NotFrozenException
     *             if the index has not been frozen. was deleted successfully
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @since 0.0
     */
    public byte delete(O object) throws NotFrozenException, DatabaseException;

}
