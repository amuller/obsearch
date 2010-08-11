package net.obsearch.utils;

import hep.aida.bin.StaticBin1D;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map.Entry;

import net.obsearch.index.utils.IntegerHolder;

public class MultiSet<O> {
	
	private HashMap<O, IntegerHolder> set;
	
	public MultiSet(int count){
		set = new HashMap<O, IntegerHolder>(count);
	}
	
	/**
	 * Add the given object to the multiset.
	 * @param object
	 */
	public void add(O object){
		IntegerHolder h = set.get(object);
		if(h == null){
			h = new IntegerHolder(0);
			set.put(object, h);
		}
		h.inc();
	}
	
	/**
	 * Return the cardinality for the given object.
	 * @param object
	 * @return
	 */
	public int cardinality(O object){
		int res = 0;
		IntegerHolder h = set.get(object);
		if(h != null){
			res = h.getValue();
		}
		return res;
	}

	public boolean containsKey(Object key) {
		return set.containsKey(key);
	}

	public Set<Entry<O, IntegerHolder>> entrySet() {
		return set.entrySet();
	}

	public boolean isEmpty() {
		return set.isEmpty();
	}

	public Set<O> keySet() {
		return set.keySet();
	}
	
	/**
	 * Return the statistical summary of the multi-set
	 * @return
	 */
	public StaticBin1D getDistribution(){
		StaticBin1D res = new StaticBin1D();
		
		for(Entry<O, IntegerHolder> e : entrySet()){
			res.add(e.getValue().getValue());
		}
		return res;	
	}

}
