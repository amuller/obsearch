package net.obsearch.index.utils;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C) 2007 Kyushu Institute of Technology

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
 * This class holds an integer.
 * @author Arnoldo Jose Muller Molina
 * @since 0
 */
public class IntegerHolder implements Comparable<IntegerHolder> {

    /**
     * Integer value to be stored.
     */
    private int value;

    /**
     * @return the value of the integer
     */
    public int getValue() {
        return value;
    }

    /**
     * Sets the value of the internal integer.
     * @param value
     *                The new value.
     */
    public void setValue(int value) {
        this.value = value;
    }

    /**
     * Public constructor.
     * @param value
     *                the value to set.
     */
    public IntegerHolder(int value) {
        super();
        this.value = value;
    }

    /**
     * Increment the integer by 1.
     */
    public void inc() {
        value++;
    }

    /**
     * Decrement the integer by 1.
     */
    public void dec() {
        value--;
    }

    /**
     * @return String representation of the integer.
     */
    public String toString() {
        return value + "";
    }

    /**
     * adds the value of x to this object's integer value.
     * @param x
     *                the value to add.
     */
    public void add(int x) {
        value += x;
    }

    /**
     * Compares this IntegerHolder to another IntegerHolder.
     */
    public int compareTo(IntegerHolder o) {
        IntegerHolder i = (IntegerHolder) o;
        int res = 0;
        if (value < i.value) {
            res = -1; 
        } else if (value > i.value) {
            res = 1;
        }
        return res;
    }

}
