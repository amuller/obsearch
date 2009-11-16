package net.obsearch.query;

import java.util.List;

import net.obsearch.AbstractOBResult;
import net.obsearch.exception.OBException;

/**
 * Encapsulates functionality common to all queries. 
 * @author amuller
 *
 * @param <O>
 */
public abstract class AbstractOBQuery<O> {
	
	
	/**
	 * Adds an object into the queue. The underlying
	 * classes calculate the distance of the object and the query.
	 * @param id id number of the object to add.
	 * @param object object added.
	 * @return true if the range has changed after the add. 
	 */
	public abstract boolean add(long id, O object) throws InstantiationException, IllegalAccessException, OBException;
	
	
	
	
	/**
	 * @return a list of objects ordered from smallest distance to largest distance
	 */
	public abstract List<AbstractOBResult<O>> getSortedElements();
	
	
	/**
	 * calculates the ep value of this query against
	 * the "DB" (all the objects of the DB sorted by distance to this query).
	 * @param db all the objects of the DB sorted by distance to this query
	 * @return ep value
	 */
	public abstract double ep(List<AbstractOBResult<O>> db);
	
	
	
	/**
	 * calculates the recall value of this query against
	 * the "DB" (all the objects of the DB sorted by distance to this query).
	 * @param db all the objects of the DB sorted by distance to this query
	 * @return ep value
	 */
	public abstract double recall(List<AbstractOBResult<O>> db);
	
	public abstract boolean isFull();
	
	

}
