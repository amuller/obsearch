package org.ajmm.obsearch.index;

import hep.aida.bin.QuantileBin1D;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.DatabaseException;

public abstract class AbstractPPTree<O extends OB> extends AbstractExtendedPyramidIndex<O> {

	@Override
	protected void updateMedianHolder(TupleInput in,
			QuantileBin1D[] medianHolder) throws OutOfRangeException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getSerializedName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void insertFromBtoC() throws DatabaseException,
			OutOfRangeException {
		// TODO Auto-generated method stub

	}

	@Override
	protected byte insertFrozen(OB object, int id) throws IllegalIdException,
			OBException, DatabaseException, OBException,
			IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void insertInB(int id, OB object) throws OBException,
			DatabaseException {
		// TODO Auto-generated method stub

	}

	@Override
	protected Index returnSelf() {
		// TODO Auto-generated method stub
		return null;
	}

}
