package net.obsearch.filter;

import java.util.LinkedList;
import java.util.List;

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
 * FilterAnd returns true if and only if each if its
 * filters return true.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public final class FilterAnd<O extends OB> implements Filter<O> {

	private List<Filter<O>> andList;
	
	public FilterAnd(){
		andList = new LinkedList<Filter<O>>();
	}

	public boolean accept(O dbObject, O query) {
		for(Filter<O> f : andList){
			if(! f.accept(dbObject, query)){
				return false;
			}
		}
		return true;
	}
	
	public void addFilter(Filter<O> f){
		andList.add(f);
	}
}
