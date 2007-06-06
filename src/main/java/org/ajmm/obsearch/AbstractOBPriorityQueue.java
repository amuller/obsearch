package org.ajmm.obsearch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

/**
 * A class that holds a list of results in an inverted
 * priority queue (biggest element at the "peek" of the queue)
 * 
 * @author amuller
 * 
 */
public abstract class AbstractOBPriorityQueue<O extends AbstractOBResult> {
    private static final Logger logger = Logger
            .getLogger(AbstractOBPriorityQueue.class);

    protected PriorityQueue<O> queue;

    protected byte k;
    
    /**
     * Creates a priority queue of k = 3
     *
     */
    public AbstractOBPriorityQueue() {
        queue = new PriorityQueue<O>(3);
    }
    /**
     * Creates a priority queue by setting the
     * maximum number of elements to be accepted
     * @param k maximum number of elements to accept
     */
    public AbstractOBPriorityQueue(byte k) {
        queue = new PriorityQueue<O>();
        this.k = k;
    }

    public int getSize() {
        return queue.size();
    }

    
    
    

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
