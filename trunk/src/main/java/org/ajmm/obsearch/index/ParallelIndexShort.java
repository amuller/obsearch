package org.ajmm.obsearch.index;

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

/**
 * This class has to be improved, please do not use it. It is not related to
 * OBSearch's p2p module.
 * @author amuller
 * @param <O>
 *            The type of object to be stored in the Index.
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
    private static final transient  Logger logger = Logger
            .getLogger(ParallelIndexShort.class);

    /**
     * Initializes this parallel index with the given index.
     * @param index
     *            The underlying index
     * @param cpus
     *            The number of cpus to use
     * @param queueSize
     *            The maximum size of items to hold in the queue
     */
    public ParallelIndexShort(IndexShort < O > index, int cpus, int queueSize) {
        super(cpus);
        this.index = index;
        queue = new LinkedBlockingQueue < OBQueryShort < O >>(queueSize);
        super.initiateThreads();
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
    protected ParallelIndex getMe() {
        return this;
    }

    /**
     * Main method, were all the processing gets done. This should be created in
     * another class, and not in this class. This is wrong, so please don't use
     * this class.
     */
    @Override
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
                this.recordedException = e;
            }
        }
    }

    /**
     * This method enqueues the given object to be matched later WARNING: never
     * call this method after calling waitQueries(). Anyway you should not be
     * able to do it if you use this index from a single thread.
     * @param object
     *            The object that has to be searched
     * @param r
     *            The range to be used
     * @param result
     *            A priority queue that will hold the result
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws IllegalIdException
     *             This exception is left as a Debug flag. If you receive this
     *             exception please report the problem to:
     *             http://code.google.com/p/obsearch/issues/list
     */
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        checkException();
        boolean interrupted = true;
        while (interrupted) {
            try {
                queue.put(new OBQueryShort < O >(object, r, result));
                counter.incrementAndGet();
                interrupted = false;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

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
