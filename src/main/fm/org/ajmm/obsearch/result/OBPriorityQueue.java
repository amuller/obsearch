<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="OBPriorityQueue"+Type+".java" />
package org.ajmm.obsearch.result;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.ob.OB${Type};
/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

 
public class OBPriorityQueue${Type}<O extends OB${Type}> extends AbstractOBPriorityQueue<OBResult${Type}<O>> {

		public OBPriorityQueue${Type}(byte k){
				super(k);
		}
		/**
     * Add the given object, object id and distance of type ${type} to the
     * queue.
		 * @param id The id of the object to be used
		 * @param obj The object to be added
		 * @param distance The distance to be added
		 */
	public void add(int id, O obj, ${type} distance) throws InstantiationException, IllegalAccessException {
        if (queue.size() == k) {
            // recycle objects.
            if (queue.peek().getDistance() >= distance) {// biggest object in
                                                            // the heap is
                                                            // bigger than d
            	OBResult${Type}<O> c = queue.poll();
                c.setDistance(distance);
                c.setObject(obj);
                c.setId(id);
                queue.offer(c);
            }
        } else { // if we are smaller than k we just create the object
        	OBResult${Type}<O> c = new OBResult${Type}<O>();
        	c.setDistance(distance);
            c.setObject(obj);
            c.setId(id);
            queue.offer(c);
        }
        assert queue.size() <= k;
    }
	
	/**
     * if queue.size() == k, then if the user's range is greater than the
     * greatest element of the queue, we can reduce the range to the biggest
     * element of the priority queue, that is its queue.peek() element
     * Returns the new range or the old range if the above condition is not met
     */
    public ${type} updateRange(${type} r) {
        // TODO: update the pyramid technique range so that we reduce the searches in the
        // remaining pyramids. We could start actually matching random pyramids
        // and then hope we can get a very small r at the beginning
        // if so, the other pyramids will be cheaper to search.
        // in paralell mode we could take the first 2 * d queries and then match
        // one pyramid by one each of the queries waiting to get the sub result, update the range
        // and then continue... this can potentially improve performance.
        if (this.getSize() == k) {
            ${type} d = queue.peek().getDistance();
            if (d < r) {
                return d; // return d 
            }           
        }
        return r; // return d if we cannot safely reduce the range
    }
    
    /**
     * Returns true if the given distance can be a candidate
     * @param d
     * @return
     */
    public boolean isCandidate(${type} d){
    	if(this.getSize() == k ){
    		// d should be less than the biggest candiate
    		// to be considered
    		return d < queue.peek().getDistance();
    	}
    	// if the queue is smaller than k,
    	// everybody is a candidate
    	return true; 
    }
	
}


</#list>