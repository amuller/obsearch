package net.obsearch.index.dprime;

import cern.colt.list.LongArrayList;
import net.obsearch.OB;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.pivot.AbstractPivotOBIndex;
import net.obsearch.pivots.IncrementalPivotSelector;

public abstract class AbstractBucketIndex<O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractPivotOBIndex<O> {

	public AbstractBucketIndex(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
	}

	/**
	 * If elementSource == null returns id, otherwise it returns
	 * elementSource[id]
	 * 
	 * @return
	 */
	protected long idMap(long id, LongArrayList elementSource) throws OBException {
		OBAsserts.chkAssert(id <= Integer.MAX_VALUE,
				"id for this stage must be smaller than 2^32");
		if (elementSource == null) {
			return id;
		} else {
			return elementSource.get((int) id);
		}
	}

	/**
	 * Auxiliary function used in freeze to get objects directly from the DB, or
	 * by using an array of object ids.
	 */
	protected O getObjectFreeze(long id, LongArrayList elementSource)
			throws IllegalIdException, IllegalAccessException,
			InstantiationException, OutOfRangeException, OBException {
			
				return getObject(idMap(id, elementSource));
			
			}

	/**
	 * Returns the bucket information for the given object.
	 * 
	 * @param object
	 *            The object that will be calculated
	 * @return The bucket information for the given object.
	 * @throws IllegalAccessException 
	 */
	protected abstract B getBucket(O object) throws OBException, InstantiationException, IllegalAccessException;

}