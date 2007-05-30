package org.ajmm.obsearch.index;

import java.lang.ref.SoftReference;

import cern.colt.map.AbstractIntDoubleMap;
import cern.colt.map.AbstractIntObjectMap;
import cern.colt.map.OpenIntDoubleHashMap;
import cern.colt.map.OpenIntObjectHashMap;

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
	  By using soft references, an OB cache is implemented
	  The garbage collector decides based on the access patterns of each
      reference, which elements are released and which are kept.
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/

public class OBCache < O > {
    
    AbstractIntObjectMap map;
    
    public OBCache(int currentDBSize){
        // using open addressing because it is cheaper
        map = new OpenIntObjectHashMap(2 * currentDBSize, 0 , 0.5);
    }
    
    /**
     * Stores the object in the cache
     * @param id
     * @param object
     */
    public void put(int id, O object){
        map.put(id, new SoftReference<O>(object));
    }
    
    /**
     * Gets the given object, returns null if the object is not found
     * @param id
     * @return
     */
    public O get(int id){
        SoftReference<O> ref = (SoftReference<O>)map.get(id);
        if(ref == null){
            return null; // the object is not here.       
         }
        O result = ref.get(); 
        if(result == null){
            map.removeKey(id); // do we really need to remove the object?
        }
        return result;
    }
    
}
