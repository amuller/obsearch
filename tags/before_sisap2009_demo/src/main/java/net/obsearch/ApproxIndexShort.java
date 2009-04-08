package net.obsearch;

import java.util.List;

import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;

/**
 * An approximate index has extra search methods useful to compare the approx
 * index against other indexes. We add search bounded by ep and precision.
 * As soon as the search reaches the given ep or precision the search
 * stops and then statistics can be fetched.
 * @author Arnoldo Jose Muller Molina
 * 
 */
public interface ApproxIndexShort<O extends OBShort> {

		
	/**
     * Searches the Index and returns OBResult (ID, OB and distance)
     * elements that are closer to "object". The closest element is at the
     * beginning of the list and the farthest elements is at the end of the
     * list. You can control the size of the resulting set when you create
     * the object "result". This becomes the k parameter of the search.
     * The search stops when the query's ep value is <= ep.
     * @param object
     *            The object that has to be searched
     * @param r
     *            The range to be used
     * @param result
     *            A priority queue that will hold the result
     * @param ep We will search until the given ep is fulfilled.
     * @param db The entire db objects sorted by distance to object
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws IllegalIdException
     *             This exception is left as a Debug flag. If you receive
     *             this exception please report the problem to:
     *             http://code.google.com/p/obsearch/issues/list
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds
     *             the range defined by the user.
     */
	void searchOBAnalyzeEp(O object, short r, OBPriorityQueueShort<O> result, double ep, List<AbstractOBResult<O>> db)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException;
	
	
	/**
     * Searches the Index and returns OBResult (ID, OB and distance)
     * elements that are closer to "object". The closest element is at the
     * beginning of the list and the farthest elements is at the end of the
     * list. You can control the size of the resulting set when you create
     * the object "result". This becomes the k parameter of the search.
     * The search stops when the query's recall is >= recall
     * @param object
     *            The object that has to be searched
     * @param r
     *            The range to be used
     * @param result
     *            A priority queue that will hold the result
     * @param recall The expected recall of the query.
     * @param perfectQueryResult the perfect query result to compare against the approx query.
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws IllegalIdException
     *             This exception is left as a Debug flag. If you receive
     *             this exception please report the problem to:
     *             http://code.google.com/p/obsearch/issues/list
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds
     *             the range defined by the user.
     */
	void searchOBAnalyzeRecall(O object, short r, OBPriorityQueueShort<O> result, double recall, List<AbstractOBResult<O>> perfectQueryResult)
	throws NotFrozenException, InstantiationException,
	IllegalIdException, IllegalAccessException, OutOfRangeException,
	OBException;

}
