package net.obsearch.pivots;

import java.util.List;

import net.obsearch.utils.Pair;

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
 * PivotResult returns a set of pivots selected from the database.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public class PivotPairResult<O> {
	
	
	
	private List<Pair<O,O>> pairs;
	
	/**
	 * Construct a pivotResult form a set of pivotIds.
	 * @param pivotIds
	 */
	public PivotPairResult(List<Pair<O,O>> pairs) {
		this.pairs = pairs;
		
	}

	public List<Pair<O, O>> getPairs() {
		return pairs;
	}



}
