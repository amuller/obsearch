package net.obsearch.pivots.random;

import java.util.Random;

import cern.colt.list.LongArrayList;
import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.pivots.AbstractIncrementalPivotSelector;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;

public class RandomPivotSelector<O extends OB> extends AbstractIncrementalPivotSelector<O> implements IncrementalPivotSelector<O> {

	public RandomPivotSelector(Pivotable<O> pivotable) {
		super(pivotable);
	}

	@Override
	public PivotResult generatePivots(int pivotCount, LongArrayList elements,
			Index<O> index) throws OBException, IllegalAccessException,
			InstantiationException, OBStorageException,
			PivotsUnavailableException {
		Random r = new Random();
		long [] ids = super.select(pivotCount, r, elements, index, null);
		return new PivotResult(ids);
	}
	
	
}
