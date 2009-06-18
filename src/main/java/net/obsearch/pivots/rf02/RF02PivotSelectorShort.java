package net.obsearch.pivots.rf02;

import cern.colt.list.LongArrayList;
import net.obsearch.Index;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.ob.OBShort;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;

public class RF02PivotSelectorShort<O extends OBShort> extends AbstractIncrementalRF02<O> {

	public RF02PivotSelectorShort(Pivotable<O> pivotable) {
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
