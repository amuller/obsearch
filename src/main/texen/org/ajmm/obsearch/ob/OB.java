<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="OB"+Type+".java" />

package org.ajmm.obsearch.ob;
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
	  This interface defines the contract for distance functions
		that return ${type} objects
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       0.0
*/
public interface OB${Type} extends OB{

		/**
     * Calculates the similarity of "this" and "object". The function
     * must satisfy the triangular inequality and return a $type.
     * 
     * @param object
     *            The object to be compared
     * @param result
     *            The resulting distance
     * @since 0.0
     */
    ${type} distance(OB object) throws OBException;
}
</#list>
