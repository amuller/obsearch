package org.ajmm.obsearch.index.pivotselection;

import java.util.Random;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.PivotSelector;

import com.sleepycat.je.DatabaseException;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * This class selects n random pivots from the database.
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class RandomPivotSelector implements PivotSelector {

    /**
     * Selects n random pivots from the database.
     * @param index
     *            The index that will be processed.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @see org.ajmm.obsearch.index.PivotSelector#generatePivots(short)
     */
    public final void generatePivots(AbstractPivotIndex index) throws OBException,
            IllegalAccessException, InstantiationException, DatabaseException {
        short pivots = index.getPivotsCount();
        int maxIdAvailable = index.getMaxId();
        final int maxId = maxIdAvailable + 1;
        int[] res = new int[pivots];
        Random r = new Random();
        int i = 0;
        while (i < res.length) {
            res[i] = r.nextInt(maxId);
            i++;
        }
        index.storePivots(res);
    }

}
