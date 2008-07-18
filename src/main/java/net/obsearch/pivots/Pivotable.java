package net.obsearch.pivots;

import net.obsearch.OB;
import net.obsearch.exception.OBException;


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
* This interface is used by some pivot selectors to determine if
* a given object can be used as a pivot. There can be distance functions
* for which some objects are more expensive to compute than others.
* For example, trees. The bigger the tree is, the more expensive
* it is to compute its distance. 
* @param <O>
*            Type of object of the index to be analyzed.
* @author Arnoldo Jose Muller Molina
* @since 0.8
*/
public interface Pivotable <O extends OB> {

    /**
     * Returns true if the given object can be used as a pivot.
     * @param object 
     * @return true if the object should be used as a pivot
     * @throws OBException If something goes wrong
     */
    boolean canBeUsedAsPivot(O object) throws OBException;
    
}
