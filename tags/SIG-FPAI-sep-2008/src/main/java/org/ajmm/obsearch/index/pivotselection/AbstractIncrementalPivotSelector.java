package org.ajmm.obsearch.index.pivotselection;

import java.util.concurrent.atomic.AtomicInteger;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.IncrementalPivotSelector;

import cern.colt.list.IntArrayList;

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
    protected final O getObject(int i, IntArrayList elements, Index<O> index)
            throws IllegalAccessException, InstantiationException,
            DatabaseException, IllegalIdException, OBException {

        return index.getObject(mapId(i, elements));
    }
    
    protected int mapId(int i, IntArrayList elements) {
        if (elements != null) {
            return elements.get(i);
        }else{
            return i;
        }
    }
    
    public int[] generatePivots(short pivotsCount, Index<O> index) throws OBException,
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
    protected int max(IntArrayList source, Index < O > index)throws OBStorageException, DatabaseException{
        int max;
        if (source == null) {
            max = index.databaseSize();
        } else {
            max = source.size();            
        }
        return max;
    }

}
