package net.obsearch.index.utils;

import net.obsearch.Index;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;

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
 * ShortUtils contains useful functions for shorts.
 * @author Arnoldo Jose Muller Molina
 */

public class ShortUtils {

    /**
     * Calculates the smap tuple for the given objectId, and the given pivots
     * @param pivots
     * @param objectId
     * @return
     */
    public static short[] getTuple(long[] pivots, long objectId,
            Index<? extends OBShort>  index) throws 
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException

    {
        short[] res = new short[pivots.length];
        int i = 0;
        OBShort o = index.getObject(objectId);
        for (long pivotId : pivots) {
            res[i] = index.getObject(pivotId).distance(o);
            i++;
        }
        return res;
    }
    
    /**
     * Calculates L-inf for two short tuples.
     * @param a  tuple
     * @param b tuple
     * @return L-infinite for a and b.
     */
    public static short lInfinite(short[] a, short[] b){
        assert a.length == b.length;
        short max = Short.MIN_VALUE;
        int i = 0;
        while(i < a.length){
            short t = (short)Math.abs(a[i]  - b[i]);
            if(t > max){
                max = t;
            }
            i++;
        }
        return max;
    }

}
