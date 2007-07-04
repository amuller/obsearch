package org.ajmm.obsearch.index.pivotselection;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.PivotSelector;

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
 * This class receives the maximum ID available in the database (before freeze)
 * and generates random pivot numbers that are inside this range
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class RandomPivotSelector implements PivotSelector {

    /**
     * Selects a number of pivots from the database.
     * 
     * @param pivots
     *            The number of pivots to be selected
     * @return A list of object ids from the database
     * @see org.ajmm.obsearch.index.PivotSelector#generatePivots(short)
     */
    public void generatePivots(AbstractPivotIndex index ) throws OBException, IllegalAccessException, InstantiationException, DatabaseException{
    	short pivots = index.getPivotsCount();
    	int maxIdAvailable = index.getMaxId();
        final int maxId = maxIdAvailable + 1;
        int[] res = new int[pivots];
        Random r = new Random();
        int i = 0;
        while (i < res.length) {
            res[i] = r.nextInt(maxId);
            i++;
        }
        index.storePivots(res);
    }
    
    

}
