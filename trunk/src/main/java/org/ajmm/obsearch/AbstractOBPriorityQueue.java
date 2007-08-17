package org.ajmm.obsearch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PriorityQueue;


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
 * This class stores matching results. It is used to minimize the amount of
 * distance computations required. To optimize a bit, the same priority queue is
 * used to store results for the user
 * @param <O>
 *            Result object to be used by the queue
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public abstract class AbstractOBPriorityQueue < O extends AbstractOBResult > {

    /**
     * The internal queue used to store results.
     */
    protected PriorityQueue < O > queue;

    /**
     * Maximum number of elements to be used. Note that this value is byte. Many
     * elements would make OBSearch very slow.
     */
    protected byte k;

    /**
     * Returns an interator with all the elements in no particular order.
     * @return Iterator with all the value
     */
    public final Iterator < O > iterator() {
        return queue.iterator();
    }

    /**
     * Creates a priority queue of k = 3.
     */
    public AbstractOBPriorityQueue() {
        queue = new PriorityQueue < O >(3);
    }

    /**
     * Creates a priority queue by setting the maximum number of elements to be
     * accepted.
     * @param k
     *            maximum number of elements to accept
     */
    public AbstractOBPriorityQueue(final byte k) {
        queue = new PriorityQueue < O >();
        this.k = k;
    }

    /**
     * @return The size of the elements of this queue.
     */
    public final int getSize() {
        return queue.size();
    }

    /**
     * @return The k of the given queue.
     */
    public final byte getK() {
        return k;
    }

    /**
     * Same "sort" of objects means that the distances of the included objects
     * are the same, and the repetitions of such distances are the same.
     * @param obj
     *            The AbstractOBPriorityQueue that will be compared.
     * @return True if the given AbstractOBPriorityQueue contains the same
     *         "sort" of objects.
     */
    @Override
    public final boolean equals(final Object obj) {
        if(obj == null){
            return false;
        }
        if(! (obj instanceof AbstractOBPriorityQueue)){
            return false;
        }
        final AbstractOBPriorityQueue < O > o = (AbstractOBPriorityQueue < O >) obj;
        if (this.getSize() != o.getSize()) {
            return false;
        }
        final Object[] a = queue.toArray();
        final Object[] b = o.queue.toArray();
        Arrays.sort(a);
        Arrays.sort(b);
        return Arrays.equals(a, b);
    }

    /**
     * @return A string representation of the queue for debugging purposes.
     */
    @Override
    public final String toString() {
        return queue.toString();
    }

}
