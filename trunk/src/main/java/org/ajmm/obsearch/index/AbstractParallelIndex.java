package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.ParallelIndex;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleInput;
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
 * AbstractParallelIndex is a wrapper class that allows to exploit all the cpus
 * of a computer with any OB index. Currently this class
 * is not ready to be used. This class is not related to the GSOC goals.
 * @param <O>
 *            The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
// TODO: There is a performance bottleneck in this class. CPU usage for a quad
// core machine gets
// only to 280% when using 4 threads. It should be 400%.
// It might be berkeley db. It might be simply the fact that there is only one
// hard drive, and 4 cpus
// eating a lot of data. Another possibility is that the queue is too slow, that
// the bottleneck is the creation of slices...
// I will work on this in the future.
// This class is officially suspended. We don't need it right now anyway.
public abstract class AbstractParallelIndex < O extends OB > implements
        Index < O >, ParallelIndex < O >, Runnable {

    /**
     * # of threads to be used.
     */
    protected int cpus;

    /**
     * Object that executes threads.
     */
    protected Executor executor;

    /**
     * Keep track of an exception if it occurs.
     */
    protected Exception recordedException;

    /**
     * variable that holds the # of unprocessed elements a search increments it
     * by one a completed search decrements it by one.
     */
    protected AtomicInteger counter;

    /**
     * Logger.
     */
    private static final transient  Logger logger = Logger
            .getLogger(AbstractParallelIndex.class);

    /**
     * Initializes this parallel index with an Index, a paralellism level.
     * @param cpus number of cpus to use.
     */
    public AbstractParallelIndex(int cpus) {
        this.cpus = cpus;
        recordedException = null;
        executor = Executors.newFixedThreadPool(cpus);
        counter = new AtomicInteger();
    }

    /**
     * This method must be called by daughters of this class when they are ready
     * to start matching.
     */
    protected void initiateThreads() {
        int i = 0;
        while (i < cpus) {
            executor.execute(getMe());
            i++;
        }
    }

    /**
     * Returns this parallel index.
     * @return This
     */
    protected abstract ParallelIndex getMe();

    /**
     * Checks if an exception was registered and if so, throws it.
     * @throws OBException
     */
    protected void checkException() throws OBException {
        if (recordedException != null) {
            logger.fatal("Exception caught in Parallel Index",
                    recordedException);
            throw new OBException(recordedException);
        }
    }

    /**
     * Returns the index that this class is parallelizing.
     * @return Internal index
     */
    public abstract Index < O > getIndex();

    public int delete(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        return getIndex().delete(object);
    }

    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException,
            UndefinedPivotsException {
        getIndex().freeze();
    }

    public O getObject(int i) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        return getIndex().getObject(i);
    }

    public int totalBoxes() {
        return getIndex().totalBoxes();
    }

    public int getBox(O object) throws OBException {
        return getIndex().getBox(object);
    }

    public int insert(O object) throws IllegalIdException, DatabaseException,
            OBException, IllegalAccessException, InstantiationException {
        int res = getIndex().insert(object);
        return res;
    }

    public boolean isFrozen() {
        return getIndex().isFrozen();
    }

    /**
     * This method is in charge of continuously wait for items to match and
     * perform the respective match.
     */
    public abstract void run();

    /**
     * Returns the count of elements found in this queue.
     * @return count of elements found in this queue.
     */
    public abstract int elementsInQueue();

    /**
     * Waits until there are no more items to be matched.
     * @throws OBException if an error occurs during match.
     */
    public final void waitQueries() throws OBException {
        while (counter.get() != 0) {
            try {
                checkException();
                // waiting.acquire();
                synchronized (counter) {
                    counter.wait();
                }

            } catch (InterruptedException e) {
            }
        }

    }

    public void close() throws DatabaseException {
        getIndex().close();
    }

    /**
     * Returns the xml of the index embedded in this ParallelIndex
     */
    public String toXML() {
        return getIndex().toXML();
    }

    public void relocateInitialize(File dbPath) throws DatabaseException,
            NotFrozenException, DatabaseException, IllegalAccessException,
            InstantiationException, OBException, IOException {
        getIndex().relocateInitialize(dbPath);
    }

    public O readObject(TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        return getIndex().readObject(in);
    }

    public boolean exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        return getIndex().exists(object);
    }
}
