package org.ajmm.obsearch.index.pivotselection;

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
		From a database, takes the first n pivots
		This pivot selector is only for testing purposes
    @author      Arnoldo Jose Muller Molina
    @version     %I%, %G%
    @since       1.0
*/


public class FixedPivotSelector implements PivotSelector {


	int [] pivotArray = {143410,
			3400,
			308101,
			132807,
			146392,
			322786,
			37130,
			284923,
			241765,
			234087,
			209606,
			46464,
			5242,
			321523,
			317796,
			69782,
			176869,
			27139,
			188754,
			73739,
			109576,
			229099,
			153514,
			163651,
			110404,
			97472,
			41835,
			41897,
			224014,
			7069,
			218288,
			297527,
			258698,
			70361,
			142473,
			52390,
			338779,
			133903,
			295473,
			136266,
			100321,
			290021,
			70388,
			17683,
			46664,
			135066,
			259983,
			75949,
			87947,
			280334,
			118411,
			271720,
			299177,
			278809,
			213894,
			204054,
			37368,
			218265,
			98567,
			13866,
			165293,
			222216,
			136266,
			316674,
			336036,
			179049,
			211876,
			80309,
			65684,
			44838,
			90099,
			231611,
			156736,
			159865,
			47891,
			218668,
			10592,
			218178,
			47988
			};
        /**
         * Selects the first n pivots in the included array.
         * This class is only for testing purposes
         * n = pivots
         * @param pivots
         *            The number of pivots to be selected
         * @return A list of object ids from the database
         * @see org.ajmm.obsearch.index.PivotSelector#generatePivots(short)
         */
        public void generatePivots(AbstractPivotIndex index) throws OBException, IllegalAccessException, InstantiationException, DatabaseException{
        	byte pivots = index.getPivotsCount();
        	int maxIdAvailable = index.getMaxId();
            assert pivots <= maxIdAvailable;
            int[] res = new int[pivots];
            int i = 0;
            while (i < res.length) {
            	assert pivotArray[i] <= maxIdAvailable;
                res[i] = pivotArray[i];
                i++;
            }
            index.storePivots(res);
        }
}
