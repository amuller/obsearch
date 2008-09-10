package net.obsearch.cache;

import net.obsearch.exception.OBStorageException;

import com.sleepycat.je.DatabaseException;

public interface OBCacheHandler<O> {

	/**
	 * Returns the size of the DB
	 * @return the size of the DB.
	 */
	public abstract long getDBSize() throws 
			OBStorageException;

}