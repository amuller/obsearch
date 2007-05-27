package org.ajmm.obsearch;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

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
 * An object to be stored in an Index must comply with this interface A distance
 * must be defined and also means of encoding and decoding the object from a
 * byte representation. Objects that implement this interface will be created by
 * using the default constructor and initialized by the load method.
 * 
 * @param <D>
 *            The type of units returned by the distance function
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public interface OB<D extends Dim> extends Storable{
    

    /**
     * Calculates the similarity of "this" object and "object". The function
     * must satisfy the triangular inequality.
     * 
     * @param object
     *            The object to be compared
     * @param result
     *            The resulting distance
     * @since 0.0
     */
    void distance(OB object, D result);
    
    /**
     * Returns the dimension type for this object.
     * OB needs this method to instantiate objects of your desired dimension type
     * You should do something like: "return Yourclass.class"
     * TODO: try to find a way of removing this method
     * @return A class object for D
     */
    Class<D> getDimensionType();
    
}
