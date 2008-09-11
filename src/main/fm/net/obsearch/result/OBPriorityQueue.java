<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="OBPriorityQueue"+Type+".java" />
package net.obsearch.result;

import net.obsearch.AbstractOBPriorityQueue;
import net.obsearch.ob.OB${Type};
import java.util.Iterator;

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
 * This is a class used to efficiently perform k-nn searches. This queue is
 * meant to be used with objects OB${Type}.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

    public final class OBPriorityQueue${Type}<O extends OB${Type}> extends AbstractOBPriorityQueue<OBResult${Type}<O>> {

    /**
     * Create the priority queue with k elements. This is how you set the k
     * for a query.
     */
    public OBPriorityQueue${Type}(int k){
        super(k);
    }
    /**
     * Add the given object, object id and distance of type ${type} to the
     * queue.
     * @param id
     *            The id of the object to be used
     * @param obj
     *            The object to be added
     * @param distance
     *            The distance to be added
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    public void add(long id, O obj, ${type} distance) throws InstantiationException, IllegalAccessException {
        if (queue.size() == k) {
            // recycle objects.
            if (queue.peek().getDistance() >= distance) {// biggest object in
                // the heap is
                // bigger than d
                OBResult${Type}<O> c = queue.poll();
                c.setDistance(distance);
                c.setObject(obj);
                c.setId(id);
								//								assert validateAddition(c);
                queue.offer(c);
            }
        } else { // if we are smaller than k we just create the object
            OBResult${Type}<O> c = new OBResult${Type}<O>();
            c.setDistance(distance);
            c.setObject(obj);
            c.setId(id);
						// assert validateAddition(c);
            queue.offer(c);
        }
        assert queue.size() <= k;
    }

		/**
		 * Make sure no repeated elements are added
		 */
		private boolean validateAddition(OBResult${Type}<O> c){
				Iterator<OBResult${Type}<O>> it = iterator();
				while(it.hasNext()){
						OBResult${Type}<O> t = it.next();
						if(t.getId() == c.getId()){
								return false;
						}
				}
				return false;
		}

    /**
     * If queue.size() == k, then if the user's range is greater than the
     * greatest element of the queue, we can reduce the range to the biggest
     * element of the priority queue, that is its queue.peek() element.
     * @param r
     *            The new range we want to calculate.
     * @return the new range or the old range if the above condition is not
     *         met
     */
    public ${type} updateRange(${type} r) {
        // TODO: update the pyramid technique range so that we reduce the
        // searches in the
        // remaining pyramids. We could start actually matching random pyramids
        // and then hope we can get a very small r at the beginning
        // if so, the other pyramids will be cheaper to search.
        // in paralell mode we could take the first 2 * d queries and then match
        // one pyramid by one each of the queries waiting to get the sub result,
        // update the range
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
     * Returns true if the given distance can be a candidate for adding it into
     * the queue. The parameter d is an estimation of the real distance, and
     * this method is used to decide if we should calculate the real distance.
     * @param d
     *            The lower resolution distance.
     * @return True if we should calculate the real distance and attempt add the
     *         corresponding object into this queue.
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
