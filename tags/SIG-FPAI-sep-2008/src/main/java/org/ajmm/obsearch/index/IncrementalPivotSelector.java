package org.ajmm.obsearch.index;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;

import cern.colt.list.IntArrayList;



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
 * Objects that implement the IncrementalPivotSelector interface are
 * expected to take objects from a) all the database, b) a subset of the database.
 * The pivot selector should return a list with all the objects that will be
 * pivots. 
 * @author Arnoldo Jose Muller Molina
 */
// TODO unify IncrementalPivotSelector and PivotSelector, find common
// functionality and create a better interface.
public interface IncrementalPivotSelector<O extends OB> {
    
    /**
     * Generates pivots from all the elements found in the DB.
     * @param pivotCount The # of pivots that will be generated.
     * @return A list of the  ids of the pivots.
     * @throws OBStorageException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws PivotsUnavailableException
     *             If not all the pivots requested were found.
     */
    int[] generatePivots(short pivotCount, Index<O> index) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException;

    /**
     * Generates pivots from the elements (object ids) given in the array elements.
     * Generates pivotsCount  pivots.
     * @param pivotCount
     * @param elements
     * @return A list of the ids of the pivots.
     */
    int[] generatePivots(short pivotCount, IntArrayList elements,  Index<O> index) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException; 
}
