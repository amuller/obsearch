package org.ajmm.obsearch;

import org.ajmm.obsearch.exception.OBException;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * An object to be stored in an Index must comply with this interface A distance
 * must be defined and also means of encoding and decoding the object from a
 * byte representation. Objects that implement this interface will be created by
 * using the default constructor and initialized by the load method. Exceptions
 * generated by the user should extend from OBException
 * 
 * WARNING: The equals method *must* be implemented. The equals does not have to
 * be true when the distance returns 0. Note however that at search time,
 * elements of distance 0 are treated in the same way. Equals is only used when
 * the database is queried for the existence of an object.
 * 
 * @param <D>
 *            The type of units returned by the distance function
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public interface OB extends Storable {

}
