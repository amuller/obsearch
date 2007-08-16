package org.ajmm.obsearch.index.sync;

import java.io.Serializable;
import java.util.Comparator;

import com.sleepycat.bind.tuple.TupleInput;

/**
 * Class used to sort the B-tree
 * @author amuller
 */
public class IntLongComparator implements Comparator, Serializable {

    public IntLongComparator() {
    }

    public static final long serialVersionUID = 1;

    public int compare(Object d1, Object d2) {

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
