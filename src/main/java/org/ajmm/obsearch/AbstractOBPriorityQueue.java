package org.ajmm.obsearch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.log4j.Logger;

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
	  Class: AbstractOBPriorityQueue
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/

public abstract class AbstractOBPriorityQueue<O extends AbstractOBResult>{
    private static final Logger logger = Logger
            .getLogger(AbstractOBPriorityQueue.class);

    protected PriorityQueue<O> queue;

    protected byte k;
    
    public Iterator<O> iterator(){
    	return queue.iterator();
    }

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
