package org.ajmm.obsearch;

import java.util.Date;
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
	  A SynchronizableIndex can be used to perform syncrhonizations with other
    indexes. We use timestamps as the base for this process.
		Someone has to guarantee that the clocks are somewhat syncrhonized if 
    the indexes were generated in different computers.
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       0.0
*/

/**
 * A boxed index is an index that divides the data into boxes.
 * In this way boxes can be distributed among different computers.
 * For n boxes, the box id is in the range [0 , n-1].
 * @param <O> The object that will be indexed
 */
public interface SynchronizableIndex<O extends OB> extends Index<O> {
	
	
	/**
	 * Returns the total # of boxes this index can potentially hold
	 * @return
	 */
	int totalBoxes();
	
	/**
	 * Returns a list of the currently held boxes for this index
	 * @return
	 */
	int [] currentBoxes() throws DatabaseException, OBException;
	
	
	/**
	 * Returns the most recent insert /delete date for the given box
	 * The resulting long is actually a date as returned by
	 * System.currentTimeMillis()
	 * Returns -1 if no data is found for the given box
	 * @param box 
	 * @return Latest inserted time in System.currentTimeMillis() format.
	 */
	long latestModification(int box) throws DatabaseException,  OBException;
	
	/**
	 * Returns an iterator with all the inserted elements newer than
	 * the given date
	 * @param x date in the format returned by System.currentTimeMillis()
	 * @param Box to search
	 * @return Iterator 
	 */
	Iterator<O> elementsInsertedNewerThan(int box, long x) throws DatabaseException,  OBException;
	
	Index<O> getIndex();
}
