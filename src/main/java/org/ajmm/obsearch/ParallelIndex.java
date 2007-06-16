package org.ajmm.obsearch;

import org.ajmm.obsearch.exception.OBException;

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
	A ParallelIndex is an Index wrapper.
	Whenever you perform a search the control will return inmediately to the caller
	and the matching will be performed in background.
	A synchronization method called waitQueries() is provided.
	When the caller calls it, the control will return until all the submitted queries
	have completed
    @author      Arnoldo Jose Muller Molina
    @version     %I%, %G%
    @since       0.0
*/

public interface ParallelIndex<O extends OB> extends Runnable {

	/**
	 * Waits for all the current searches to complete.
	 * @throws OBException
	 */
	void waitQueries() throws OBException;

	Index<O> getIndex();



}
