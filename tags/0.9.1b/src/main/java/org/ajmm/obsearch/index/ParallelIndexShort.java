package org.ajmm.obsearch.index;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.ParallelIndex;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

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
 * A parallel index for OBShort indexes.
 * @param <O>
 *                The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class ParallelIndexShort < O extends OBShort >
        extends AbstractParallelIndex < O > implements IndexShort < O > {

    /**
     * Queue of the data that will be queried.
     */
    private BlockingQueue < OBQueryShort < O >> queue;

    /**
     * Underlying Index.
     */
    private IndexShort < O > index;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(ParallelIndexShort.class);

    /**
     * Initializes this parallel index with the given index.
     * @param index
     *                The underlying index
     * @param cpus
     *                The number of cpus to use
     */
    public ParallelIndexShort(IndexShort < O > index, int cpus) {
        super(cpus);
        this.index = index;
        queue = new ArrayBlockingQueue < OBQueryShort < O >>(cpus);
        initiateThreads();
    }
    
    /**
     * This method must be called by daughters of this class when they are ready
     * to start matching.
     */
    protected void initiateThreads() {
        int i = 0;
        while (i < cpus) {
            executor.execute(new Matcher());
            i++;
        }
    }
    
  
    
    public String getSerializedName(){
        return this.getClass().getSimpleName();
    }

    @Override
    public int elementsInQueue() {
        return queue.size();
    }

    @Override
    public Index < O > getIndex() {
        return index;
    }

    public int databaseSize() throws DatabaseException {
        return this.getIndex().databaseSize();
    }

    @Override
    protected ParallelIndex<O> getMe() {
        return this;
    }

    private class Matcher implements Runnable {
        /**
         * Main method, were all the processing gets done. This should be
         * created in another class, and not in this class. This is wrong, so
         * please don't use this class.
         */
       
        public void run() {
            // this method takes an element from the queue, and matches it.
            // the user is responsible of storing result values in a safe place.
            // this is repeated forever
            while (true) {
                try {
                    OBQueryShort < O > toMatch = queue.take();
                    index.searchOB(toMatch.getObject(), toMatch.getDistance(),
                            toMatch.getResult());
                    if (counter.decrementAndGet() == 0) {
                        // only one thread is waiting on this object
                        synchronized (counter) {
                            counter.notifyAll();
                        }
                    }
                } catch (InterruptedException e) {
                } catch (Exception e) {
                    // something went wrong. We won't recover at this point
                    recordedException = e;
                }
            }
        }
    }

    /**
     * This method enqueues the given object to be matched later WARNING: never
     * call this method after calling waitQueries(). Anyway you should not be
     * able to do it if you use this index from a single thread.
     * @param object
     *                The object that has to be searched
     * @param r
     *                The range to be used
     * @param result
     *                A priority queue that will hold the result
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     */
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        checkException();
        boolean interrupted = true;
        //while (interrupted) {
            try {
                queue.put(new OBQueryShort < O >(object, r, result));
                counter.incrementAndGet();
          //      interrupted = false;
            } catch (InterruptedException e) {
           //     interrupted = true;
            }
       // }

    }

    public boolean intersects(O object, short r, int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        return index.intersects(object, r, box);
    }

    public int[] intersectingBoxes(O object, short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        return index.intersectingBoxes(object, r);
    }

    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        index.searchOB(object, r, result, boxes);
    }

}
