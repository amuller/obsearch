package org.ajmm.obsearch.index.pivotselection;
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


public class DummyPivotSelector implements PivotSelector {
    
        
        
        /**
         * Selects the first n pivots from the database
         * n = pivots
         * @param pivots
         *            The number of pivots to be selected
         * @return A list of object ids from the database
         * @see org.ajmm.obsearch.index.pivotselection.PivotSelector#generatePivots(short)
         */
        public int[] generatePivots(short pivots, int maxIdAvailable) {
            assert pivots <= maxIdAvailable;
            int[] res = new int[pivots];
            int i = 0;
            while (i < res.length) {
                res[i] = i;
                i++;
            }
            return res;
        }  
}
