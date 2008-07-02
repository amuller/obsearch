package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.OperationStatus;
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
 * UnsafeNCorePPTreeShort Implementation of a P+Tree that stores OB objects
 * whose distance functions generate shorts. We take the burden of maintaining
 * one class per data-type for efficiency reasons. <b>Warning:</b> This class
 * uses an undocumented class called Unsafe. The databases created with this
 * index might not be portable between systems (endianness). The advantage is
 * this index runs faster for big k and big r. This class also parallelizes
 * certain operations, taking advantage of multi-core architectures. The
 * implementation tries to be gentle with the secondary storage to avoid costly
 * random accesses.
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *                The type of object to be stored in the Index.
 * @since 0.7
 */
public class UnsafeNCorePPTreeShort < O extends OBShort >
        extends UnsafePPTreeShort < O > {

    private static final transient Logger logger = Logger
            .getLogger(UnsafePPTreeShort.class);

    /**
     * Number of CPUS available to this index. Performs some parallelizations in
     * memory-bound parts of the algorithm.
     */
    private int cpus = 4;

    /**
     * Here we store the requests for distance functions
     */
    private transient ArrayBlockingQueue < DistanceEvaluation > queue;

    /**
     * Lock used to wait until the last distance has been calculated.
     */
    private transient Semaphore waitRemainingElements = new Semaphore(0);

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
     * @param type The class of the object O that will be used.  
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public UnsafeNCorePPTreeShort(File databaseDirectory, short pivots,
            byte od, PivotSelector < O > pivotSelector, int cpus, Class<O> type)
            throws DatabaseException, IOException, OBException {
        this(databaseDirectory, pivots, od, Short.MIN_VALUE, Short.MAX_VALUE,
                pivotSelector, cpus, type);
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
     * @param cpus
     *                Number of cpus to use.
     * @param type The class of the object O that will be used.  
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public UnsafeNCorePPTreeShort(File databaseDirectory, short pivots,
            byte od, short minInput, short maxInput,
            PivotSelector < O > pivotSelector, int cpus, Class<O> type)
            throws DatabaseException, IOException, OBException {
        super(databaseDirectory, pivots, od, minInput, maxInput, pivotSelector, type);

        this.cpus = cpus;
        initNCoreHacks();
    }

    private void initNCoreHacks() {

        tupleHackIndex = new AtomicInteger(pivotsCount);

        hackS = new Semaphore(0);

        ex = null;

        waitRemainingElements = new Semaphore(0);

        int i = 0;
        while (i < cpus) {
            Thread x = new Thread(new PivotTupleCalculation());
            x.start();
            i++;
        }
        /*
         * queue = new ArrayBlockingQueue < DistanceEvaluation >(cpus * 5000); i =
         * 0; // leave one thread for smap, and the rest for distance
         * calculations. int total = cpus - 1; if (cpus == 1) { total = 1; }
         * while (i < 1) { Thread x = new Thread(new DistanceCalculation());
         * x.start(); i++; }
         */
    }

    public void relocateInitialize(final File dbPath) throws DatabaseException,
            NotFrozenException, DatabaseException, IllegalAccessException,
            InstantiationException, OBException, IOException {
        initNCoreHacks();
        super.relocateInitialize(dbPath);
    }

    /**
     * This class stores temporary results.
     */
    private final class DistanceEvaluation {
        private short r;

        private int id;

        private O queryObject;

        private OBPriorityQueueShort < O > result;

        private O toCompare;

        public DistanceEvaluation(short r, int id, O toCompare, O queryObject,
                OBPriorityQueueShort < O > result) {
            super();
            setAll(r, id, toCompare, queryObject, result);
        }

        public void setAll(short r, int id, O toCompare, O queryObject,
                OBPriorityQueueShort < O > result) {
            this.r = r;
            this.toCompare = toCompare;
            this.queryObject = queryObject;
            this.result = result;
            this.id = id;
        }

        public void match() throws DatabaseException, IllegalAccessException,
                InstantiationException, IllegalIdException, OBException {

            short realDistance = queryObject.distance(toCompare);
            if (realDistance <= r) {
                result.add(id, toCompare, realDistance);
            }

        }

    }

    /**
     * This class helps to perform the distance calculations
     */
    private class DistanceCalculation implements Runnable {
        public void run() {
            while (true) {
                try {
                    DistanceEvaluation ev = queue.take();
                    ev.match();
                    // we know that if "we are done" and the queue is empty, no
                    // other
                    // guy will be added to the queue.
                } catch (InterruptedException e) {

                } catch (Exception e) {
                    logger.fatal(e);
                    queue = null;
                }
            }
        }
    }

    /**
     * Calculates the tuple vector for the given object.
     * @param obj
     *                object to be processed
     * @param tuple
     *                The resulting tuple will be stored here
     */

    protected final void calculatePivotTuple(final O obj, short[] tuple)
            throws OBException {
        // This method must be synchronized
        synchronized (tupleHackIndex) {
            assert tuple.length == this.pivotsCount;
            if (ex != null) {
                assert ex != null;
                ex.printStackTrace();
                throw new OBException(ex);
            }
            tupleHack = tuple;
            tupleHackCurrentObject = obj;
            tupleHackIndex.set(0);
            tupleHackCompleted = new CountDownLatch(pivotsCount);
            hackS.release(pivotsCount);
            try {
                tupleHackCompleted.await();
            } catch (InterruptedException e) {

            }
            assert hackS.availablePermits() == 0;
            assert tupleHackIndex.get() == pivotsCount;
            assert validateTuple(obj, tuple);
        }
    }

    /**
     * Returns true if the result of the multi-thread method is the same as the
     * method in the super class
     * @param obj
     * @param tuple
     * @return
     */
    private boolean validateTuple(O obj, short[] tuple) throws OBException {
        short[] tuple2 = new short[pivotsCount];
        super.calculatePivotTuple(obj, tuple2);
        assert Arrays.equals(tuple, tuple2) : " Concurrently created array : "
                + Arrays.toString(tuple)
                + " not equal to serially created array: "
                + Arrays.toString(tuple2);
        return Arrays.equals(tuple, tuple2);
    }

    /**
     * This is a temporary hack value used to calculate the pivots in parallel.
     */
    private transient short[] tupleHack;

    private transient O tupleHackCurrentObject;

    private transient AtomicInteger tupleHackIndex = new AtomicInteger(
            pivotsCount);

    private transient Semaphore hackS = new Semaphore(0);

    private transient Exception ex = null;

    private transient CountDownLatch tupleHackCompleted;

    /**
     * This class helps to perform the pivot calculation in parallel.
     */
    private class PivotTupleCalculation implements Runnable {
        public void run() {
            while (true) {
                hackS.acquireUninterruptibly();
                int i = tupleHackIndex.getAndIncrement();
                if (i < pivotsCount) {
                    try {
                        // assert tupleHackCurrentObject != null;
                        assert pivots != null;
                        assert tupleHackCurrentObject != null;
                        assert tupleHack != null;
                        // logger.debug("Pivot calculation " + i);
                        tupleHack[i] = tupleHackCurrentObject
                                .distance(pivots[i]);

                        tupleHackCompleted.countDown();
                    } catch (Exception e) {
                        logger.fatal(e);
                        ex = e;
                    }
                }
            }
        }
    }

    /**
     * This method reads from the B-tree applies l-infinite to discard false
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
    /*
     * TODO: parallelizing this would imply that more distance computations have
     * to be performed. public final void searchBTreeAndUpdate(O object, short[]
     * tuple, short r, float hlow, float hhigh, OBPriorityQueueShort < O >
     * result) throws DatabaseException, IllegalAccessException,
     * InstantiationException, IllegalIdException, OBException { Cursor cursor =
     * null; try { DatabaseEntry keyEntry = new DatabaseEntry(); DatabaseEntry
     * dataEntry = new DatabaseEntry(); cursor = cDB.openCursor(null, null);
     * SortedFloatBinding.floatToEntry(hlow, keyEntry); OperationStatus retVal =
     * cursor.getSearchKeyRange(keyEntry, dataEntry, null); if (retVal ==
     * OperationStatus.NOTFOUND) { return; } if (cursor.count() > 0) { float
     * currentPyramidValue = SortedFloatBinding .entryToFloat(keyEntry); short
     * max = Short.MIN_VALUE; Unsafe unsafe = UnsafeArrayHandlerShort.unsafe;
     * long i = 0; long init = UnsafeArrayHandlerInt.size +
     * UnsafeArrayHandlerByte.offset; short t; while (retVal ==
     * OperationStatus.SUCCESS && currentPyramidValue <= hhigh) { byte[] in =
     * dataEntry.getData(); // TupleInput in = new
     * TupleInput(dataEntry.getData()); this.smapRecordsCompared++; i = init;
     * max = Short.MIN_VALUE; // STATS for (short b : tuple) { t = (short)
     * Math.abs(b - unsafe.getShort(in, i)); if (t > max) { max = t; if (t > r) {
     * break; // finish this loop this slice won't be // matched // after all! } }
     * i += UnsafeArrayHandlerShort.size; } if (max <= r &&
     * result.isCandidate(max)) { // there is a chance it is a possible match
     * int id = intHandler.getint(in, 0); O toCompare = getObject(id);
     * DistanceEvaluation d = new DistanceEvaluation(r, id, toCompare, object,
     * result); while (true) { try { queue.put(d); break; } catch
     * (InterruptedException e) { } } this.distanceComputations++; } // read the
     * next record retVal = cursor.getNext(keyEntry, dataEntry, null); // update
     * the current pyramid value so that we know when // to // stop
     * currentPyramidValue = SortedFloatBinding .entryToFloat(keyEntry); } } }
     * finally { cursor.close(); } }
     */
    public int getCpus() {
        return cpus;
    }

    public void setCpus(int cpus) {
        this.cpus = cpus;
    }

}
