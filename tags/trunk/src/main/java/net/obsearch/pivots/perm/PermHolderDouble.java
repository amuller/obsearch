package net.obsearch.pivots.perm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.index.perm.impl.PerDouble;

/**
 * Holds and sorts objects, creates hash codes and sorts the permutations.
 * @author amuller
 *
 */
public class PermHolderDouble {
	
	private List<PerDouble> perms;
	
	
	public PermHolderDouble(int size){
		perms = new ArrayList<PerDouble>(size);
	}
	
	public void addPer(PerDouble p){
		perms.add(p);
		Collections.sort(perms);
	}
	
	public boolean equals(Object o){
		PermHolderDouble d  = (PermHolderDouble)o;		
		return perms.equals(d.perms);
	}
	
		
	public int distance(PermHolderDouble b) {
		int i = 0;
		int res = 0; 
		assert perms.size() == b.perms.size();
		while(i < perms.size()){
			res += Math.abs(perms.get(i).getId() - b.perms.get(i).getId());
			i++;
		}
		return res;
	}
	
	/**
	 * set the given pivot id with the new permutation
	 * @param pivotId
	 * @param newPerm
	 * @throws OBException 
	 */
	public void set(short pivotId, PerDouble newPerm) throws OBException{
		int i =0;
		boolean changed = false;
		OBAsserts.chkAssert(pivotId == newPerm.getId(), "ids do not match");
		while(i < perms.size()){
			if(perms.get(i).getId() == pivotId){
				perms.set(i, newPerm);
				changed = true;
			}
			i++;
		}
		if(! changed){
			perms.add(newPerm);
		}
		Collections.sort(perms);
	}
	
	public int hashCode(){
		return perms.hashCode();
	}
}
