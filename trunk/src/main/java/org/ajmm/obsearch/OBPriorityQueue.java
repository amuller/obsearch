package org.ajmm.obsearch;

import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

/**
 * A class that holds a list of FuriaChanCandidate 
 * in a priority queue
 * @author amuller
 *
 */
public class OBPriorityQueue < R extends Result<D>, D extends Dim> {
	private static final Logger logger = Logger.getLogger(OBPriorityQueue.class);
	protected PriorityQueue<R> queue;
	protected byte k;
	public OBPriorityQueue(byte k){
		queue = new PriorityQueue<R>();
		this.k = k;
	}
	
	public int  getSize(){
		return queue.size();
	}
	
	public D getBiggestDistance(){
		return queue.peek().getDistance();
	}
	
	/**
	 * Receives the distance and returns true if we should skip a
     * record with distance d
	 * @param d
	 * @return
	 */
	/*public synchronized boolean  shouldSkipRecord(final D d){
		if (getSize() == k){
			return ! (getBiggestDistance().gt(d));
		}else{
			return false;
		}
	}*/
	
	public void add(R d) throws InstantiationException, IllegalAccessException{
		if(queue.size() == k){
			// otherwise we recycle objects!
			if( getBiggestDistance().gt(d.getDistance())){//biggest object in the heap is bigger than d
				R c = queue.poll();
				c.set(d);
				queue.offer(c);
			}
		}else{ // if we are smaller than k we just create the object
            R c = (R)d.getClass().newInstance();
            c.set(d);
			queue.offer(d);
		}
		assert queue.size() <= k;
	}
    
    /**
     * if queue.size() == k, then if the user's range is greater
     * than the greatest element of the queue, we can reduce the size
     * of the range to something smaller than the current biggest value of the queue
     */
    public void updateRange(D r){
        D d = queue.peek().getDistance();
        if(queue.size() == k && d.lt(r)){
            r.updateSmaller(d);
        }
        assert r.lt(d);
    }
	
	
}
