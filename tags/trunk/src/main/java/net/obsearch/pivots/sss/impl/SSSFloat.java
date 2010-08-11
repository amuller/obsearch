package net.obsearch.pivots.sss.impl;

import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.Pivotable;
import net.obsearch.pivots.sss.AbstractMySSS;

public class SSSFloat<O extends OBFloat> extends AbstractMySSS<O>implements IncrementalPivotSelector<O>  {

	public SSSFloat(Pivotable<O> pivotable) {
		super(pivotable);
	}

	@Override
	protected double distance(O o1, O o2) throws OBException {
		return o1.distance(o2);
	}

	

}
