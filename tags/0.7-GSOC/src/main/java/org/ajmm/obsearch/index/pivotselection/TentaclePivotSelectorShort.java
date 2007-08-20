package org.ajmm.obsearch.index.pivotselection;

import hep.aida.bin.MightyStaticBin1D;

import java.util.Random;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.ob.OBShort;
import org.apache.log4j.Logger;

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
 * This class selects n pivots that are at least d or {@link #minD} units away
 * from each other. d is calculated by taking one random pivot p. We then
 * calculate d by computing the average distance of p and all the elements
 * stored in the database. If this cannot be satisfied, then we increase d until
 * it satisfies.
 * @param <O>
 *            The object that is stored in the index we want to process.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public class TentaclePivotSelectorShort < O extends OBShort >
        extends AbstractTentaclePivotSelector < O > {
    /**
     * The value of d.
     */
    private short d = Short.MIN_VALUE;

    /**
     * The minimum distance we are willing to accept.
     */
    private short minD;

    /**
     * Logger.
     */
    private static final transient  Logger logger = Logger
            .getLogger(TentaclePivotSelectorShort.class);

    /**
     * Creates a new tentacle selector that will select pivots with at least
     * minD units.
     * @param minD
     *            (minimun accepted number of units)
     */
    public TentaclePivotSelectorShort(short minD) {
        this.minD = minD;
    }

    @Override
    protected boolean easifyD() {
        if (d > minD) {
            d--;
            if (logger.isDebugEnabled()) {
                logger.debug("Current d: " + d);
            }
            return true;
        } else {
            return false;
        }

    }

    @Override
    protected O obtainD(AbstractPivotIndex < O > x)
            throws InstantiationException, IllegalAccessException,
            DatabaseException, OBException {
        Random r = new Random();
        int m = x.getMaxId();
        int id = r.nextInt(m);
        O ob = x.getObject(id);
        int i = 0;
        MightyStaticBin1D data = new MightyStaticBin1D(true, false, 4);
        while (i < m) {
            if (id != i) {
                short res = ob.distance(x.getObject(i));
                if (logger.isDebugEnabled()) {
                    if (i % 10000 == 0) {
                        logger.debug("Finding averages for:" + i);
                    }
                }
                if (res != 0) {
                    data.add(res);
                }
            }
            i++;
        }
        double mean = data.geometricMean();

        d = (short) mean;
        logger.debug("D found by harmonic mean: " + d);
        return ob;
    }

    @Override
    protected boolean withinRange(O a, O b) throws OBException {
        return Math.abs(a.distance(b) - d) <= minD;
    }

}
