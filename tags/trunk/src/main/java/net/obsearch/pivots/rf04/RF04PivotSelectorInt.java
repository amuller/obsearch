package net.obsearch.pivots.rf04;

import cern.colt.list.LongArrayList;
import net.obsearch.Index;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.ob.OBFloat;
import net.obsearch.ob.OBInt;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;

public class RF04PivotSelectorInt<O extends OBInt> extends AbstractIncrementalRF04<O> {

	public RF04PivotSelectorInt(Pivotable<O> pivotable) {
		super(pivotable);		
	}

	@Override
	protected double distance(O a, O b) throws OBException {
		return a.distance(b);
	}

	@Override
	public PivotResult generatePivots(int pivotCount, LongArrayList elements,
			Index<O> index) throws OBException, IllegalAccessException,
			InstantiationException, OBStorageException,
			PivotsUnavailableException {
		
		return null;
	}
	
	

}
