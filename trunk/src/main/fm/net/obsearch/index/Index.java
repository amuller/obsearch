<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="Index"+Type+".java" />

package net.obsearch.index;
import net.obsearch.Index;
import net.obsearch.ob.OB${Type};
import net.obsearch.result.OBPriorityQueue${Type};
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OutOfRangeException;
import java.util.Iterator;
import net.obsearch.filter.Filter;


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
 * An Index interface for distance functions that return ${Type}s.
 * @param <O>
 *            An object of type OB${Type} that will be stored in the index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public interface Index${Type}<O extends OB${Type}> extends Index<O> {
	     /**
         * Searches the Index and returns OBResult (ID, OB and distance)
         * elements that are closer to "object". The closest element is at the
         * beginning of the list and the farthest elements is at the end of the
         * list. You can control the size of the resulting set when you create
         * the object "result". This becomes the k parameter of the search.
         * @param object
         *            The object that has to be searched
         * @param r
         *            The range to be used
         * @param result
         *            A priority queue that will hold the result
         * @throws NotFrozenException
         *             if the index has not been frozen.
         * @throws OBException
         *             User generated exception
         * @throws IllegalAccessException
         *             If there is a problem when instantiating objects O
         * @throws InstantiationException
         *             If there is a problem when instantiating objects O
         * @throws IllegalIdException
         *             This exception is left as a Debug flag. If you receive
         *             this exception please report the problem to:
         *             http://code.google.com/p/obsearch/issues/list
         * @throws OutOfRangeException
         *             If the distance of any object to any other object exceeds
         *             the range defined by the user.
         */
    
    void searchOB(O object, ${type} r, OBPriorityQueue${Type}<O> result)
            throws NotFrozenException,
            InstantiationException, IllegalIdException, IllegalAccessException, OutOfRangeException, OBException;


		 /**
         * Searches the Index and returns OBResult (ID, OB and distance)
         * elements that are closer to "object". The closest element is at the
         * beginning of the list and the farthest elements is at the end of the
         * list. You can control the size of the resulting set when you create
         * the object "result". This becomes the k parameter of the search.
				 * The parameter "filter" is used to remove unwanted objects from 
         * the result (a select where clause). Users are responsible to
         * implement at least one filter that can be used with their O.
         * @param object
         *            The object that has to be searched
         * @param r
         *            The range to be used
         * @param result
         *            A priority queue that will hold the result
         * @throws NotFrozenException
         *             if the index has not been frozen.
         * @throws OBException
         *             User generated exception
         * @throws IllegalAccessException
         *             If there is a problem when instantiating objects O
         * @throws InstantiationException
         *             If there is a problem when instantiating objects O
         * @throws IllegalIdException
         *             This exception is left as a Debug flag. If you receive
         *             this exception please report the problem to:
         *             http://code.google.com/p/obsearch/issues/list
         * @throws OutOfRangeException
         *             If the distance of any object to any other object exceeds
         *             the range defined by the user.
         */
    
    void searchOB(O object, ${type} r, Filter<O> filter, OBPriorityQueue${Type}<O> result)
            throws NotFrozenException,
            InstantiationException, IllegalIdException, IllegalAccessException, OutOfRangeException, OBException;

		    
/**
	 * This method returns a list of all the distances of the query against  the DB.
	 * This helps to calculate EP values in a cheaper way. results that are equal to the original object are added or skipped based on "filterSame"
	 * as Float.MAX_VALUE
	 * @param query
	 * @param filterSame if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
		public ${type}[] fullMatchLite(O query, boolean filterSame) throws OBException, IllegalAccessException, InstantiationException;
		
}

</#list>
