package net.obsearch.pivots.bustos.impl;

import java.util.Arrays;
import java.util.HashMap;

import hep.aida.bin.StaticBin1D;

import net.obsearch.Index;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.index.utils.ShortUtils;
import net.obsearch.pivots.Pivotable;
import net.obsearch.pivots.bustos.AbstractIncrementalBustosNavarroChavez;

import net.obsearch.ob.OBShort;

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
 * IncrementalBustosNavarroChavezShort is an implementation 
 * for OBShort objects
 * @author Arnoldo Jose Muller Molina
 */

public class IncrementalBustosNavarroChavezShort<O extends OBShort>
        extends AbstractIncrementalBustosNavarroChavez<O> {
    
    /**
     * Keeps track of the SMAP values of objects.
     */
    private HashMap<Long, short[]> smapCache;
    
    /**
     * Receives the object that accepts pivots as possible candidates. Selects l
     * pairs of objects to compare which set of pivots is better, and selects m
     * possible pivot candidates from the data set.
     * @param pivotable
     * @param l pairs of objects to select
     * @param m m possible pivot candidates to be randomly picked.
     */
    public IncrementalBustosNavarroChavezShort(Pivotable < O > pivotable,
            int l, int m) {
        super(pivotable, l , m);
        smapCache = new HashMap<Long, short[]>(l);
    }

    @Override
    protected double calculateMedian(long[] pivots, long[] x, long[] y, Index<O> index) throws DatabaseException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBException{
        StaticBin1D data = new StaticBin1D();  
        int i = 0;
        while ( i < x.length){
            short[] tupleA = getTuple(pivots, x[i], (Index<OBShort>)index);
            short[] tupleB = getTuple(pivots, y[i], (Index<OBShort>)index);
            short distance = ShortUtils.lInfinite(tupleA, tupleB);
            data.add(distance);
            i++;
        }                
        return data.mean();
    }
    
    protected void resetCache(int x){
        smapCache = new HashMap<Long, short[]>(x);
    }
    
    private short[] getTuple(long[] pivots, long id, Index<OBShort>index  )throws DatabaseException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBException{
        short[] t = smapCache.get(id);
        if(t == null){
            t = new short[pivots.length];
            smapCache.put(id, t);
            assert pivots.length == 1;
        }else{          
            short [] td = new short[pivots.length];
            System.arraycopy(t, 0, td, 0, t.length );
            smapCache.put(id, td);
            t = td;
        }
        int i = pivots.length-1;
        t[i] = index.getObject(id).distance(index.getObject(pivots[i]));
        return t;
    }

    /* (non-Javadoc)
     * @see net.obsearch.result.index.pivotselection.AbstractIncrementalBustosNavarroChavez#validatePivots(int[], int)
     */
    @Override
    protected boolean validatePivots(long[] pivots, long id, Index<O> index) throws DatabaseException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBException {
        short[] real = ShortUtils.getTuple(pivots, id, index);
        short[] localTuple = smapCache.get(id);
        return Arrays.equals(real, localTuple);
    }
    
    

}
