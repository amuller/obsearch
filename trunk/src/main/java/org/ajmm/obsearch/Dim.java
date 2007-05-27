package org.ajmm.obsearch;

import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleInput;

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
 * An interface used to store dimension units. For now, it is an empty interface
 * just to make everything clear
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public interface Dim extends Storable {

    /**
     * This method should read one dimension a from in and do: 
     * t = Math.abs(this - a)
     * if (t > r)
     * return false;
     * if (t > max) { 
     * max = t; 
     * return true;
     * 
     * @param in the input where the dimension will be parsed
     * @param r   the range to be used
     * @param max the current maximum value
     * @return true if we should continue calculating linf, false otherwise
     */
    public boolean lInfiniteOneStep(TupleInput in, Dim r, Dim max);

}
