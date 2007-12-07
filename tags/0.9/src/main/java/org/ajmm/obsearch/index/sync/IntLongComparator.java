package org.ajmm.obsearch.index.sync;

import java.io.Serializable;
import java.util.Comparator;

import com.sleepycat.bind.tuple.TupleInput;

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
 * Class used to sort the B-tree of a SynchronizableIndex.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public class IntLongComparator implements Comparator, Serializable {

    /**
     * Serial number.
     */
    private static final long serialVersionUID = 969401225778990207L;

    /**
     * Default constructor required by Berkeley DB.
     */
    public IntLongComparator() {
    }

    /**
     * Compare two objects that hold an integer followed by a long.
     * The int represents a box number.
     * The long represents a date.
     * @param d1 object to compare
     * @param d2 object to compare
     * @return 1 if d1> d2 0 if d1 == d2 -1 if d1 < d2
     */
    public final int compare(final Object d1, final Object d2) {

        TupleInput in1 = new TupleInput((byte[]) d1);
        TupleInput in2 = new TupleInput((byte[]) d2);

        int box1 = in1.readInt();
        int box2 = in2.readInt();

        if (box1 > box2) {
            return 1;
        } else if (box1 < box2) {
            return -1;
        } else { // box1 == box2
            long date1 = in1.readLong();
            long date2 = in2.readLong();
            if (date1 > date2) {
                return 1;
            } else if (date1 < date2) {
                return -1;
            } else { // everybody is the same
                return 0;
            }
        }

    }
}
