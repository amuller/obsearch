package net.obsearch.cache;

import java.util.LinkedHashMap;

import net.obsearch.exception.OBException;

public abstract class AbstractOBCache<K, O> extends LinkedHashMap<K,O> {

	
	protected int cacheSize;
	
	public AbstractOBCache(int cacheSize){
		super(cacheSize * 2, 0.75f, true);
		this.cacheSize = cacheSize;
	}
	
	/**
	 * Makes sure the cache is cleaned and each
	 * object is stored.
	 * @throws OBException 
	 */
	public abstract void clearAll() throws OBException;
	
}
