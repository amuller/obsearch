package net.obsearch.filter;

import net.obsearch.OB;

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
 * Filter allows the user to remove unwanted results from OBSearch.
 * If the distance function is more costly than the filter, then
 * there could be performance gains for a filter.
 * Users shall provide an implementation of Filter that
 * suits their needs. The query object is provided in case
 * the filtering class provided by the user requires this information.
 *
 * @author Arnoldo Jose Muller Molina
 */

public interface Filter<O extends OB> {

	/**
	 * Returning true implies that the object "dbObject" will
	 * be included in the result set. 
	 * @param dbObject Object that is considered as a result.
	 * @param query Original query provided by the user.
	 * @return true if dbObject is to be included in the result set.
	 */
	boolean accept(O dbObject, O query);
	
}
