package net.obsearch.pivots.dummy;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.pivots.AbstractIncrementalPivotSelector;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;


import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * IncrementalDummyPivotSelector is used for testing purposes.
 * @author Arnoldo Jose Muller Molina
 */

public class IncrementalDummyPivotSelector < O extends OB >
        extends AbstractIncrementalPivotSelector < O > implements
		IncrementalPivotSelector<O> {
    
    public IncrementalDummyPivotSelector(){
        super(null);
    }

    @Override
    public PivotResult generatePivots(int pivotCount, Index<O> index)
            throws OBException, IllegalAccessException, InstantiationException,
            OBStorageException, PivotsUnavailableException {
        // TODO Auto-generated method stub
        return generatePivots(pivotCount, null, index);
    }

    
    public PivotResult generatePivots(int pivotCount, LongArrayList elements,
            Index<O> index) throws OBException, IllegalAccessException,
            InstantiationException, OBStorageException,
            PivotsUnavailableException {
        int i = 0;
        long[] res = new long[pivotCount];
        while (i < pivotCount) {
            res[i] = super.mapId(i, elements);
            i++;
        }
        return new PivotResult(res);
    }

}
