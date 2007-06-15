package org.ajmm.obsearch.index;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.ajmm.obsearch.AbstractOBResult;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.ParallelIndex;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;

import com.sleepycat.je.DatabaseException;
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
	  Wrapper class that allows to exploit all the cpus of a computer with
		any OB index. :) yay!

    @author      Arnoldo Jose Muller Molina
    @version     %I%, %G%
    @since       0.0
*/

public abstract class AbstractParallelIndex<O extends OB> implements Index<O>, ParallelIndex, Runnable {

	protected int cpus;
	protected Executor executor;
	protected Exception recordedException;

	/**
	 * Initializes this parallel index with an Index,
	 * a paralellism level
	 * @param index
	 */
	public AbstractParallelIndex(int cpus){
		this.cpus = cpus;
		recordedException = null;
		executor  = Executors.newFixedThreadPool(cpus);
		// now we execute the threads, and leave those threads
		// active for the life of the object.
		int i = 0;
		while(i < cpus){
			executor.execute(this);
			i++;
		}
	}

	protected void checkException() throws OBException{
		if(recordedException != null){
			throw new OBException(recordedException);
		}
	}

	/**
	 * Returns the index that this class is parallelizing
	 * @return Internal index
	 */
	protected abstract Index<O>getIndex();



	public byte delete(O object) throws NotFrozenException, DatabaseException {
		return getIndex().delete(object);
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			DatabaseException, OutOfRangeException, OBException,
			UndefinedPivotsException {
			getIndex().freeze();
	}

	public O getObject(int i) throws DatabaseException, IllegalIdException,
			IllegalAccessException, InstantiationException {
		return getIndex().getObject(i);
	}

	public byte insert(O object, int id) throws IllegalIdException,
			DatabaseException, OBException, IllegalAccessException,
			InstantiationException {

		return this.insert(object, id);
	}

	public boolean isFrozen() {
		return getIndex().isFrozen();
	}

	/**
	 * This method is in charge of continuously wait for items to match
	 * and perform the respective match.
	 */
	public abstract void run();

	/**
	 * Returns the elements found in this queue
	 * @return
	 */
	public abstract int elementsInQueue();

	/**
	 * Waits until there are no more items to be matched.
	 *
	 */
	public void waitQueries()  throws OBException{
		while(elementsInQueue() != 0){
			try {
				checkException();
                wait();
            } catch (InterruptedException e) {}
		}
	}




}
