package org.ajmm.obsearch.index.pivotselection;

import hep.aida.bin.StaticBin1D;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.utils.ShortUtils;
import org.ajmm.obsearch.ob.OBShort;

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
     * Receives the object that accepts pivots as possible candidates. Selects l
     * pairs of objects to compare which set of pivots is better, and selects m
     * possible pivot candidates from the data set.
     * @param pivotable
     * @param l
     * @param m
     */
    public IncrementalBustosNavarroChavezShort(Pivotable < O > pivotable,
            int l, int m) {
        super(pivotable, l , m);
    }

    @Override
    protected double calculateMedian(int[] pivots, int[] x, int[] y, Index<O> index) throws DatabaseException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBException{
        StaticBin1D data = new StaticBin1D();  
        int i = 0;
        while ( i < x.length){
            short[] tupleA = ShortUtils.getTuple(pivots, x[i], (Index<OBShort>)index);
            short[] tupleB = ShortUtils.getTuple(pivots, y[i], (Index<OBShort>)index);
            short distance = ShortUtils.lInfinite(tupleA, tupleB);
            data.add(distance);
            i++;
        }                
        return data.mean();
    }

}
