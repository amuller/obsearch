package org.ajmm.obsearch.index.pivotselection;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.PivotSelector;

import com.sleepycat.je.DatabaseException;

public class DummyPivotSelector implements PivotSelector {


	/**
     * Selects the first n pivots from the database
     * n = pivots
     * @param pivots
     *            The number of pivots to be selected
     * @return A list of object ids from the database
     * @see org.ajmm.obsearch.index.PivotSelector#generatePivots(short)
     */
    public void generatePivots(AbstractPivotIndex index) throws OBException, IllegalAccessException, InstantiationException, DatabaseException{
    	short pivots = index.getPivotsCount();
    	int maxIdAvailable = index.getMaxId();
        assert pivots <= maxIdAvailable;
        int[] res = new int[pivots];
        int i = 0;
        while (i < res.length) {
        	assert i <= maxIdAvailable;
            res[i] = i;
            i++;
        }
        index.storePivots(res);
    }
}
