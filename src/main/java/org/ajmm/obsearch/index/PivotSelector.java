package org.ajmm.obsearch.index;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.apache.log4j.Logger;

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
 * Implementations of this class have the task of selecting a subset of the
 * objects before freezing to be used as pivots. How the pivot access the
 * objects depends largely on theIndex implementation.
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */
// TODO: This class needs to have a consistent access to all the elements.
// Figure this out later as the RandomPivot doesn't need this data.
public interface PivotSelector<O extends OB> {
    /**
     * Generates n (n = pivots) from the database The resulting array is a list
     * of ids from the database
     * The method will modify the pivot index and
     * update the information of the selected new pivots
     */ 
    void generatePivots(AbstractPivotIndex<O> x) throws OBException, IllegalAccessException, InstantiationException, DatabaseException, PivotsUnavailableException;
}
