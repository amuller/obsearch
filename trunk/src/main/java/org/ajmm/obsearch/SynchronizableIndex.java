package org.ajmm.obsearch;

import java.util.Date;
import java.util.Iterator;

import org.ajmm.obsearch.exception.OBException;

import com.sleepycat.je.DatabaseException;

/**
 * A boxed index is an index that divides the data into boxes.
 * In this way boxes can be distributed among different computers.
 * For n boxes, the box id is in the range [0 , n-1].
 * @param <O> The object that will be indexed
 */
public interface SynchronizableIndex<O extends OB> extends Index<O> {
	
	
	/**
	 * Returns the total # of boxes this index can potentially hold
	 * @return
	 */
	int totalBoxes();
	
	/**
	 * Returns a list of the currently held boxes for this index
	 * @return
	 */
	int [] currentBoxes() throws DatabaseException, OBException;
	
	
	/**
	 * Returns the most recent insert /delete date for the given box
	 * The resulting long is actually a date as returned by
	 * System.currentTimeMillis()
	 * Returns -1 if no data is found for the given box
	 * @param box 
	 * @return Latest inserted time in System.currentTimeMillis() format.
	 */
	long latestModification(int box) throws DatabaseException,  OBException;
	
	/**
	 * Returns an iterator with all the inserted elements newer than
	 * the given date
	 * @param x date in the format returned by System.currentTimeMillis()
	 * @param Box to search
	 * @return Iterator 
	 */
	Iterator<O> elementsNewerThan(int box, long x) throws DatabaseException,  OBException;
	
	Index<O> getIndex();
}
