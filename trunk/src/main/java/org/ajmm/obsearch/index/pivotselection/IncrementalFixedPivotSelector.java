package org.ajmm.obsearch.index.pivotselection;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.IncrementalPivotSelector;

import cern.colt.list.IntArrayList;

public class IncrementalFixedPivotSelector
        extends FixedPivotSelector implements IncrementalPivotSelector {

    @Override
    public int[] generatePivots(short pivotCount, Index index)
            throws OBException, IllegalAccessException, InstantiationException,
            OBStorageException, PivotsUnavailableException {
        return generatePivots(pivotCount, null,null);
    }

    @Override
    public int[] generatePivots(short pivotCount, IntArrayList elements,
            Index index) throws OBException, IllegalAccessException,
            InstantiationException, OBStorageException,
            PivotsUnavailableException {
        short pivots = pivotCount;
        int[] res = new int[pivots];
        int i = 0;
        while (i < res.length) {            
            res[i] = pivotArray[i];
            i++;
        }
        return res;
    }

}
