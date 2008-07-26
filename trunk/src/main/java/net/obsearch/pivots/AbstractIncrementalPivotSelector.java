package net.obsearch.pivots;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;


import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

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
 * AbstractIncrementalPivotSelector holds common functionality to all the
 * incremental pivot selectors.
 * @author Arnoldo Jose Muller Molina
 */

public abstract class AbstractIncrementalPivotSelector < O extends OB >
        implements IncrementalPivotSelector < O > {

    
    
    protected AbstractIncrementalPivotSelector(Pivotable < O > pivotable){
        this.pivotable = pivotable;
    }
    /**
     * Pivotable objects determine if a given object is suitable. For example,
     * in the case of trees, very big trees will become a burden and we should
     * avoid using them as pivots.
     */
    protected Pivotable < O > pivotable;

    // TODO: The id auto increment must be initialized properly. We should leave
    // this
    // auto-increment to the underlying storage system.

    /**
     * Returns the given object. If elements != null, then the returned item id
     * is elements[i].
     * @param i
     *                The id in the database or in elements of the object that
     *                will be accessed.
     * @param elements
     *                Elements that will be searched.
     * @return O object of the corresponding id.
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws DatabaseException
     * @throws IllegalIdException
     * @throws OBException
     */
    protected final O getObject(long i, LongArrayList elements, Index<O> index)
            throws IllegalAccessException, InstantiationException,
            DatabaseException, IllegalIdException, OBException {

        return index.getObject(mapId(i, elements));
    }
    
    protected long mapId(long i, LongArrayList elements) {
        if (elements != null) {
            return elements.get((int)i);
        }else{
            return i;
        }
    }
    
    /* (non-Javadoc)
	 * @see net.obsearch.pivots.IncrementalPivotSelector#generatePivots(int, net.obsearch.result.Index)
	 */
    public PivotResult generatePivots(int pivotsCount, Index<O> index) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException
    {
        return generatePivots(pivotsCount,null, index);
    }
    
    /**
     * Returns the max # of elements. if source != null then source.size()
     * otherwise index.databaseSize();
     * @param source The source of data (can be null)
     * @param index The underlying index.
     * @return The max # of elements of source if source != null or of index if source == null.
     */
    protected int max(LongArrayList source, Index < O > index)throws OBStorageException{
        int max;
        if (source == null) {
            max = (int)Math.min(index.databaseSize(), Integer.MAX_VALUE);
        } else {
            max = source.size();            
        }
        return max;
    }

	/**
	 * Selects k random elements from the given source.
	 * @param k
	 *                number of elements to select
	 * @param r
	 *                Random object used to randomly select objects.
	 * @param source
	 *                The source of item ids.
	 * @param index
	 *                underlying index.
	 * @param will not add pivots included in excludes.
	 * @return The ids of selected objects.
	 */
	protected long[] select(int k, Random r, LongArrayList source,
			Index < O > index, LongArrayList excludes) throws OBStorageException
			 {
			    int max = max(source, index);
			    long[] res = new long[k];
			    int i = 0;
			    while (i < res.length) {
			        long id = mapId(r.nextInt(max), source);
			        if(excludes == null || ! excludes.contains(id)){
			            res[i] = id;
			        }else{
			            continue; // repeat step.
			        }
			        i++;
			    }
			    return res;
			}

}
