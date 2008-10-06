package net.obsearch.index;

import org.neo4j.api.core.RelationshipType;

import net.obsearch.OB;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.pivots.IncrementalPivotSelector;

public abstract class AbstractKnnGraph <O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
extends AbstractBucketIndex<O, B, Q, BC> {
	
	protected enum MyRelationshipTypes implements RelationshipType

	{
	    NN
	}


	public AbstractKnnGraph(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
	}

	
}
