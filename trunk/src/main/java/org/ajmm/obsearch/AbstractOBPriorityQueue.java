package org.ajmm.obsearch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

/**
 * A class that holds a list of FuriaChanCandidate in a priority queue
 * 
 * @author amuller
 * 
 */
public abstract class AbstractOBPriorityQueue<O extends AbstractOBResult> {
    private static final Logger logger = Logger
            .getLogger(AbstractOBPriorityQueue.class);

    protected PriorityQueue<O> queue;

    protected byte k;

    public AbstractOBPriorityQueue(byte k) {
        queue = new PriorityQueue<O>();
        this.k = k;
    }

    public int getSize() {
        return queue.size();
    }

    /*public D getBiggestDistance() {
        return queue.peek().getDistance();
    }*/

    /*

    public void add(AbstractOBResult<O,D> d) throws InstantiationException, IllegalAccessException {
        if (queue.size() == k) {
            // otherwise we recycle objects!
            if (getBiggestDistance().gt(d.getDistance())) {// biggest object in
                                                            // the heap is
                                                            // bigger than d
                AbstractOBResult<O,D> c = queue.poll();
                c.set(d);
                queue.offer(c);
            }
        } else { // if we are smaller than k we just create the object
            AbstractOBResult<O,D> c = new AbstractOBResult<O,D>();
            c.set(d);
            queue.offer(c);
        }
        assert queue.size() <= k;
    }
	*/
    
    /**
     * if queue.size() == k, then if the user's range is greater than the
     * greatest element of the queue, we can reduce the size of the range to
     * something smaller than the current biggest value of the queue
     */
    /*public void updateRange(D r) {
        // TODO: update the pyramid technique range so that we reduce the searches in the
        // remaining pyramids. We could start actually matching random pyramids
        // and then hope we can get a very small r at the beginning
        // if so, the other pyramids will be cheaper to search.
        // in paralell mode we could take the first 2 * d queries and then match
        // one pyramid by one each of the queries waiting to get the sub result, update the range
        // and then continue... this can potentially improve performance.
        if (this.getSize() == k) {
            D d = queue.peek().getDistance();
            if (d.lt(r)) {
                r.update(d);
            }           
        }
    }*/

    public boolean equals(Object obj) {
        AbstractOBPriorityQueue<O> o = (AbstractOBPriorityQueue<O>)obj;
        if (this.getSize() != o.getSize()) {
            return false;
        }
        Object[] a = queue.toArray();
        Object[] b = o.queue.toArray();
        Arrays.sort(a);
        Arrays.sort(b);
        return Arrays.equals(a, b);
        //return queue.containsAll(o.queue);
    }

    public String toString() {        
        return  queue.toString();//result.toString();
    }

}
