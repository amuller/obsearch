package org.ajmm.obsearch;

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
/**
 * Class: Result A class that holds results consisting of an object ID and the
 * respective distance The comparator is inverted so that the result objects can
 * be used directly in a priority queue
 * 
 * @param <
 *            D > Dimension type to use.
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class Result<D extends Dim> implements Comparable {
    protected int id;

    protected D distance;

    public D getDistance() {
        return distance;
    }

    public void setDistance(D d) {
        this.distance = d;
    }

    public int getId() {
        return id;
    }

    public void setId(int identification) {
        this.id = identification;
    }

    public int compareTo(Object o) {
        assert o instanceof Result; // faster!
        // if(! (o instanceof Result)){
        // throw new ClassCastException();
        // }
        return compareToAux((Result) o);
    }
    /**
     * Compares a result object but returns the value in a 
     * reversed way so that the object can be used inside
     * a priority queue
     * @param comp The Object to be compared
     * @return 1 if the other object is bigger
     *                 -1 if this object is bigger
     *                 0 if htey are equal
     */
    protected final int compareToAux(final Result comp) {
        int res = 0;
        if (distance.lt(comp.distance)) {
            res = 1;
        } else if (distance.gt(comp.distance)) {
            res = -1;
        }
        return res;
    }
    /**
     * Sets the values of this object to the values of r.
     * @param r The values that will be copied into this object.
     */
    public void set(Result<D> r){
        this.distance = r.distance;
        this.id = r.id;
    }
}
