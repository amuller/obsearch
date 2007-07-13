package org.ajmm.obsearch;

import java.util.Date;
import java.util.Iterator;

import org.ajmm.obsearch.exception.OBException;

import com.sleepycat.je.DatabaseException;

public interface TimeStampIndex<O extends OB> extends Index<O> {
	
	/**
	 * Returns the latest inserted item
	 * The resulting long is actually a date as returned by
	 * System.currentTimeMillis()
	 * @return
	 */
	long latestInsertedItem() throws DatabaseException,  OBException;
	
	/**
	 * Returns an iterator with all the elements newer than
	 * the given date
	 * @param x date in the format returned by System.currentTimeMillis()
	 * @return Iterator 
	 */
	Iterator<O> elementsNewerThan(long x) throws DatabaseException,  OBException;
	
}
