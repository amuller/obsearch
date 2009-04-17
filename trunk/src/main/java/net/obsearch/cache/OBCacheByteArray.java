package net.obsearch.cache;

import gnu.trove.TIntObjectHashMap;

import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import net.obsearch.exception.OBException;
import net.obsearch.exception.OutOfRangeException;

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

public final class OBCacheByteArray < O > extends AbstractOBCache<ByteArrayKey, O>{

	  private OBCacheHandlerByteArray < O > handler;

	    /**
	     * Initialize the cache with the given amount of elements.
	     * @param size
	     *                Number of elements that the internal hash table will be
	     *                initialized with.
	     */
	    public OBCacheByteArray(OBCacheHandlerByteArray < O > handler, int cacheSize) throws  OBException{
	       super(cacheSize);
	       this.handler = handler;
	    }


	    /**
	     * Gets the given object, returns null if the object is not found.
	     * @param id
	     *                internal id.
	     * @return null if no object is found
	     */
	    
	    public O get(byte[] id) throws  OBException, InstantiationException , IllegalAccessException {
	    	ByteArrayKey realId = new ByteArrayKey(id);
	        O obj = super.get(realId);
	        if (obj == null) {
	        	obj = handler.loadObject(id);   
	        	if(obj != null){
	        		super.put(realId, obj);
	        	}
       
	        }
	        return obj;
	    }
	    
	    public boolean exists(byte[] id){
	    	ByteArrayKey realId = new ByteArrayKey(id);
	    	return super.containsKey(realId);
	    }
	    
	    public void put(byte[] id, O object){
	    	super.put(new ByteArrayKey(id), object);
	    }


		@Override
		protected boolean removeEldestEntry(java.util.Map.Entry<ByteArrayKey, O> eldest){
			if(this.size() > cacheSize){
				// write the contents of the object.
				try{
					handler.store(eldest.getKey().getKey(), eldest.getValue());
				}catch(OBException e){
					throw new UnsupportedOperationException(e);
				}
				return true;
			}else{
				return false;
			}
		}


		@Override
		public void clearAll() throws OBException {
			for(Map.Entry<ByteArrayKey, O> e : super.entrySet()){
				handler.store(e.getKey().getKey(), e.getValue());
			}
			this.clear();
		}
	
	
		/**
		 * Remove the given key from the table.
		 * @param key
		 */
		public void remove(byte[] key){
			super.remove(new ByteArrayKey(key));
		}

    
    
    
}
