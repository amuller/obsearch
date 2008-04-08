package org.ajmm.obsearch.cache;

import gnu.trove.TIntObjectHashMap;

import java.lang.ref.SoftReference;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;

import com.sleepycat.je.DatabaseException;

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
 * released and which are kept. The cache controls the loading of items. For
 * this purpose an OBCacheLoader is provided to control the loading and
 * instantiation of the objects from secondary storage. That is why this cache
 * does not have a put method. It assumes that all the requested items exist
 * in secondary storage otherwise it returns an error. Loading operations
 * generate a lock but reading operations do not generate any locks. 
 * @param <O>
 *                The type of object that will be stored in the Cache.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public final class OBCache < O > {

    /**
     * The map that stores the cache.
     */
    // private ConcurrentHashMap< Integer,O > map;
    private OpenIntObjectHashMap map;

    private OBCacheLoader < O > loader;

    /**
     * Initialize the cache with the given amount of elements.
     * @param size
     *                Number of elements that the internal hash table will be
     *                initialized with.
     */
    public OBCache(OBCacheLoader < O > loader) throws  OBException{
        // using open addressing because it is cheaper
        try{
        map = new OpenIntObjectHashMap(2 * loader.getDBSize(), 0, 0.5);
        }catch(Exception e){
            throw new OBException(e);
        }
        // map = new ConcurrentHashMap< Integer, O >();
        // map = new TIntObjectHashMap < SoftReference<O> >(size);
        this.loader = loader;
    }

    /**
     * Removes from the cache the given id to recover some memory.
     * @param id
     *                the id to be removed
     */
    public void remove(int id) {
        map.removeKey(id);
    }
    
    public void removeAll(){
        map.clear();
    }

    /**
     * Gets the given object, returns null if the object is not found.
     * @param id
     *                internal id.
     * @return null if no object is found
     */
    public O get(final int id) throws DatabaseException, OutOfRangeException, OBException, InstantiationException , IllegalAccessException {
        // return map.get(id);
        // SoftReference<O>
        SoftReference < O > ref = (SoftReference < O >) map.get(id);
        if (ref == null || ref.get() == null) {
            // we load the object.
            synchronized (loader) {
                // we have to check if the obj is there again in
                // case someone else added it already
                ref = (SoftReference < O >) map.get(id);
                if (ref == null || ref.get() == null) {
                    ref = new SoftReference < O >(loader.loadObject(id));
                    map.put(id, ref);
                }
            }
        }
        return ref.get();
    }
}
