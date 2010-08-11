package net.obsearch.index.ghs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
/**
 * 
 * Priority queue that stores the smallest k items from a sample set.
 *
 * @param <O>
 */
public class FixedPriorityQueue<O extends Comparable<O>> implements Comparator<O>, Iterable<O>{
	
	private PriorityQueue<O> q;
	private int k;
	
	public FixedPriorityQueue(int k){
		q = new PriorityQueue<O>(k, this);
		this.k = k;
	}
	
	
	
	public void add(O e) {
		if(isFull()){
			if(q.peek().compareTo(e) > 0){
				// greatest element bigger than our element
				q.poll();
				q.offer(e);
			}
		}else{
			q.offer(e);
		}
		
	}

	public boolean isFull(){
		return q.size() == k;
	}


	public boolean offer(O e) {
		return q.offer(e);
	}



	public O peek() {
		return q.peek();
	}



	public O poll() {
		return q.poll();
	}
	
	public void addAll(O[] data){
		for(O o : data){
			add(o);
		}
	}



	@Override
	public Iterator<O> iterator() {
		
		return getSortedData().iterator(); 
	}
	
	public List<O> getSortedData(){
		ArrayList<O> res = new ArrayList<O>(k);
		for(O o : q){
			res.add(o);
		}
		Collections.sort(res);
		return res;
	}



	@Override
	public int compare(O o1, O o2) {
		int res = o1.compareTo(o2);
		assert (~res + 1) == (res * -1);
		return ~res + 1;
	}
	
	public int size(){
		return q.size();
	}

}
