package org.ajmm.obsearch.index;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;

import com.sleepycat.je.DatabaseException;

public class ParallelIndexShort<O extends OBShort> extends
		AbstractParallelIndex<O> implements IndexShort<O> {

	protected BlockingQueue<OBQueryShort<O>> queue;

	protected IndexShort<O> index;


	/**
	 * Initializes this parallel index with the given index
	 *
	 * @param index
	 *            The underlying index
	 * @param cpus
	 *            The number of cpus to use
	 * @param queueSize
	 *            The maximum size of items to hold in the queue
	 */
	public ParallelIndexShort(IndexShort<O> index, int cpus, int queueSize) {
		super(cpus);
		this.index = index;
		queue = new ArrayBlockingQueue<OBQueryShort<O>>(queueSize);
	}

	@Override
	public int elementsInQueue() {
		return queue.size();
	}

	@Override
	protected Index<O>getIndex(){
		return index;
	}

	@Override
	public void run() {
		// this method takes an element from the queue, and matches it.
		// the user is responsible of storing result values in a safe place.
		// this is repeated forever
		while (true) {
			try {
				OBQueryShort<O> toMatch = queue.take();
				index.searchOB(toMatch.getObject(), toMatch.getDistance(), toMatch.getResult());
			} catch (InterruptedException e) {
			} catch(Exception e){
				// something went wrong. We won't recover at this point
				this.recordedException = e;
			}
		}
	}

	/**
	 * This method enqueues the given object to be matched later
	 */
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		checkException();
		boolean interrupted = true;
		while (interrupted) {
			try {
				queue.put(new OBQueryShort<O>(object, r, result));
				interrupted = false;
			} catch (InterruptedException e) {
				interrupted = true;
			}
		}

	}

}
