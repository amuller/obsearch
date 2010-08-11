package net.obsearch.pivots.perm.impl;

import com.sleepycat.je.DatabaseException;

import net.obsearch.Index;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.Pivotable;
import net.obsearch.pivots.perm.AbstractIncrementalPerm;

public class IncrementalPermFloat<O extends OBFloat> extends AbstractIncrementalPerm<O> 
implements IncrementalPivotSelector<O> 
{

	public IncrementalPermFloat(Pivotable<O> pivotable, int l, int m) {
		super(pivotable, l, m);
		
	}


	@Override
	protected double distance(O a, O b) throws OBException {
		return a.distance(b);
	}





}
