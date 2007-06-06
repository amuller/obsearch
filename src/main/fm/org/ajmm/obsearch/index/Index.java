<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="Index"+Type+".java" />

package org.ajmm.obsearch.index;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.ob.OB${Type};
import org.ajmm.obsearch.result.OBPriorityQueue${Type};
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
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
		Interface that defines an Index whose distance functions
		are ${type}s
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       0.0
*/


public interface Index${Type}<O extends OB${Type}> extends Index<O> {
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
    
    void searchOB(O object, ${type} r, OBPriorityQueue${Type}<O> result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException, OutOfRangeException, OBException;
}

</#list>
