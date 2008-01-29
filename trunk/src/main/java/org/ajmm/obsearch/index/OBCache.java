package org.ajmm.obsearch.index;

import gnu.trove.TIntObjectHashMap;

import java.lang.ref.SoftReference;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import cern.colt.map.OpenIntObjectHashMap;

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
 * By using soft references, an OB cache is implemented The garbage collector
 * decides based on the access patterns of each reference, which elements are
 * released and which are kept.
 * @param <O>
 *            The type of object that will be stored in the Cache.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class OBCache < O > {

    /**
     * The map that stores the cache.
     */
    //private WeakHashMap < Integer, O  > map;
    private ConcurrentHashMap< Integer,O > map;
    /**
     * Initialize the cache with the given amount of elements.
     * @param size
     *            Number of elements that the internal hash table will be
     *            initialized with.
     */
    public OBCache(final int size) {
        // using open addressing because it is cheaper
        //map = new OpenIntObjectHashMap(2 * currentDBSize, 0 , 0.5);
        map = new ConcurrentHashMap< Integer, O >();
        //map = new WeakHashMap<Integer, O>(size);
       // map = new TIntObjectHashMap  < SoftReference<O> >(size);
    }

    /**
     * Stores the object in the cache.
     * @param id
     *            Internal id of the object
     * @param object
     *            Object to store
     */
    public final void put(final int id, final O object) {
        
        //map.put(id, new SoftReference<O>(object));
        map.put(id,object);
    }

    /**
     * Gets the given object, returns null if the object is not found.
     * @param id
     *            internal id.
     * @return null if no object is found
     */
    public final O get(final int id) {
        //return map.get(id);
         //SoftReference<O>  
        O ref = map.get(id);
         return ref;
        /*if (ref == null) {
            return null; // the object is not here.
        }
        O result = ref.get();
        return result;*/
    }
}
