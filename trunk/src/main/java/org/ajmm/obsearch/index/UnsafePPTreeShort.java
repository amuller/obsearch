package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.util.UnsafeArrayHandlerByte;
import org.ajmm.obsearch.util.UnsafeArrayHandlerInt;
import org.ajmm.obsearch.util.UnsafeArrayHandlerShort;
import org.apache.log4j.Logger;

import sun.misc.Unsafe;

import com.sleepycat.bind.tuple.SortedFloatBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

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
 * UnsafePPTreeShort Implementation of a P+Tree that stores OB objects whose
 * distance functions generate shorts. We take the burden of maintaining one
 * class per data-type for efficiency reasons. <b>Warning:</b> This class uses
 * an undocumented class called Unsafe. The databases created with this index
 * might not be portable between systems (endianess). The advantage is this
 * index runs faster for big k and big r.
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *                The type of object to be stored in the Index.
 * @since 0.7
 */
public class UnsafePPTreeShort < O extends OBShort >
        extends PPTreeShort < O > {

    private static final transient Logger logger = Logger
            .getLogger(UnsafePPTreeShort.class.getSimpleName());

    /**
     * Handles direct access to short numbers from a byte array.
     */
    protected static transient UnsafeArrayHandlerShort shortHandler = new UnsafeArrayHandlerShort();

    /**
     * Handles direct access to int numbers from a byte array.
     */
    protected static transient UnsafeArrayHandlerInt intHandler = new UnsafeArrayHandlerInt();

    /**
     * Handles direct access to byte numbers from a byte array.
     */
    protected static transient UnsafeArrayHandlerByte byteHandler = new UnsafeArrayHandlerByte();

    /**
     * Constructor.
     * @param databaseDirectory
     *                Directory were the index will be stored
     * @param pivots
     *                Number of pivots to be used.
     * @param od
     *                Partitions for the space tree (please check the P+tree
     *                paper)
     * @param pivotSelector
     *                The pivot selector that will be used by this index.
     * @param cpus
     *                Number of CPUS to use.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public UnsafePPTreeShort(File databaseDirectory, short pivots, byte od,
            PivotSelector < O > pivotSelector) throws DatabaseException,
            IOException {
        this(databaseDirectory, pivots, od, Short.MIN_VALUE, Short.MAX_VALUE,
                pivotSelector);
    }

    /**
     * Creates a new UnsafePPTreeShort. Ranges accepted by this index will be
     * defined by the user. We recommend the use of this constructor. We believe
     * it will give better resolution to the float transformation. The values
     * returned by the distance function must be within [minInput, maxInput].
     * These two values can be over estimated but not under estimated.
     * @param databaseDirectory
     *                Directory were the index will be stored
     * @param pivots
     *                Number of pivots to be used.
     * @param minInput
     *                Minimum value to be returned by the distance function
     * @param maxInput
     *                Maximum value to be returned by the distance function
     * @param pivotSelector
     *                The pivot selector that will be used by this index.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public UnsafePPTreeShort(File databaseDirectory, short pivots, byte od,
            short minInput, short maxInput, PivotSelector < O > pivotSelector)
            throws DatabaseException, IOException {
        super(databaseDirectory, pivots, od, minInput, maxInput, pivotSelector);

    }

    /**
     * This method reads from the B-tree appies l-infinite to discard false
     * positives. This technique is called SMAP. Calculates the real distance
     * and updates the result priority queue It is left public so that junit can
     * perform validations on it Performance-wise this is one of the most
     * important methods
     * @param object
     *                object to search
     * @param tuple
     *                tuple of the object
     * @param r
     *                range
     * @param hlow
     *                lowest pyramid value
     * @param hhigh
     *                highest pyramid value
     * @param result
     *                result of the search operation
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     */
    public void searchBTreeAndUpdate(O object, short[] tuple, short r,
            float hlow, float hhigh, OBPriorityQueueShort < O > result)
            throws DatabaseException, IllegalAccessException,
            InstantiationException, IllegalIdException, OBException {

        Cursor cursor = null;

        try {

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry dataEntry = new DatabaseEntry();
            cursor = cDB.openCursor(null, null);
            SortedFloatBinding.floatToEntry(hlow, keyEntry);
            OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
                    dataEntry, null);

            if (retVal == OperationStatus.NOTFOUND) {
                return;
            }

            if (cursor.count() > 0) {
                float currentPyramidValue = SortedFloatBinding
                        .entryToFloat(keyEntry);
                short max = Short.MIN_VALUE;
                short realDistance = Short.MIN_VALUE;
                Unsafe unsafe = UnsafeArrayHandlerShort.unsafe;
                long i = 0;
                long init = UnsafeArrayHandlerInt.size
                        + UnsafeArrayHandlerByte.offset;
                short t;
                while (retVal == OperationStatus.SUCCESS
                        && currentPyramidValue <= hhigh) {

                    byte[] in = dataEntry.getData();
                    // TupleInput in = new TupleInput(dataEntry.getData());
                    this.smapRecordsCompared++;

                    i = init;
                    max = Short.MIN_VALUE;
                    // STATS

                    for (short b : tuple) {
                        t = (short) Math.abs(b - unsafe.getShort(in, i));
                        if (t > max) {
                            max = t;
                            if (t > r) {
                                break; // finish this loop this slice won't be
                                // matched
                                // after all!
                            }

                        }
                        i += UnsafeArrayHandlerShort.size;
                    }

                    if (max <= r && result.isCandidate(max)) {
                        // there is a chance it is a possible match
                        int id = intHandler.getint(in, 0);
                        O toCompare = super.getObject(id);
                        realDistance = object.distance(toCompare);
                        this.distanceComputations++;
                        if (realDistance <= r) {
                            result.add(id, toCompare, realDistance);
                        }
                    }

                    // read the next record
                    retVal = cursor.getNext(keyEntry, dataEntry, null);
                    // update the current pyramid value so that we know when
                    // to
                    // stop
                    currentPyramidValue = SortedFloatBinding
                            .entryToFloat(keyEntry);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Inserts the given tuple and id into C with the given ppTreeValue.
     * @param t
     *                tuple
     * @param ppTreeValue
     *                P+Tree value for the tuple
     * @param id
     *                internal id
     * @return 1 if everything was successful
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    protected byte insertFrozenAuxAux(float ppTreeValue, short[] t, int id)
            throws DatabaseException {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        byte[] out = createCTuple();
        // write the tuple
        intHandler.putint(out, 0, id);
        int i = 0;
        for (short d : t) {
            shortHandler.putshort(out, i, UnsafeArrayHandlerInt.size, d);
            i++;
        }
        // create the key
        SortedFloatBinding.floatToEntry(ppTreeValue, keyEntry);
        dataEntry.setData(out);

        if (cDB.put(null, keyEntry, dataEntry) != OperationStatus.SUCCESS) {
            throw new DatabaseException();
        }
        return 1;
    }

    /**
     * Creates the tuple that will be used in the DB C.
     * @return A byte array that represents the tuple.
     */
    private final byte[] createCTuple() {
        return new byte[UnsafeArrayHandlerInt.size
                + (UnsafeArrayHandlerShort.size * pivotsCount)];
    }

    /**
     * Returns 0 if both tuples are the same, otherwise, it returns. the first
     * non zero pair. This is used to find pairs of tuples that are likely to be
     * similar The second tuple is made of shorts except its first item.
     * @param tuple
     * @param tuple2
     * @return the l-infinite calculation of both tuples
     */
    protected final int equalTuples(short[] tuple, byte[] tuple2) {
        int i = 0;
        long m = UnsafeArrayHandlerInt.size + UnsafeArrayHandlerByte.offset;
        // STATS
        while (i < tuple.length) {
            if (tuple[i] != UnsafeArrayHandlerShort.unsafe.getShort(tuple2, m)) {
                return -1;
            }
            m += UnsafeArrayHandlerShort.size;
            i++;
        }
        return intHandler.getint(tuple2, 0);
    }

}
