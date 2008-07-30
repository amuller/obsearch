package net.obsearch.dimension;

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
 * AbstractDimension stores a dimension (the order it is and some abstract
 * primitive value).
 * 
 * @author Arnoldo Jose Muller Molina
 */

public abstract class AbstractDimension {

	/**
	 * The position of this dimension. Note that this value could
	 * be inferred from the position in the array.
	 * However, if we want to sort dimensions by their value then it
	 * is necessary to keep this value.
	 */
	private int order;

	protected AbstractDimension(int order) {
		this.order = order;
	}

	public int getOrder() {
		return order;
	}

}
