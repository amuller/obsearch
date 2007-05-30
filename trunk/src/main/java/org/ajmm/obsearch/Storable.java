/**
 * 
 */
package org.ajmm.obsearch;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/*
 * OBSearch: a distributed similarity search engine This project is to
 * similarity search what 'bit-torrent' is to downloads. Copyright (C) 2007
 * Arnoldo Jose Muller Molina
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/**
 * Interface that abstracts the general storage and loading methods used to
 * persist objects
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public interface Storable {
    /**
     * Stores this object in a byte array.
     * 
     * @param out
     *            A TupleOutput where values can be stored
     * @since 0.0
     */
    void store(TupleOutput out);

    /**
     * Populates the object's internal properties from the given byte stream.
     * 
     * @param in
     *            A TupleInput object from where primitive types can be loaded.
     * @since 0.0
     */
    void load(TupleInput in);
}
