package org.ajmm.obsearch.index.pivotselection;

import hep.aida.bin.MightyStaticBin1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.utils.OBRandom;
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
     * Number of seeds to be used. We will take the highest harmonic value
     * found.
     */
    private int seedCount;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(TentaclePivotSelectorShort.class);

    /**
     * Creates a new tentacle selector that will select pivots with at least
     * minD units.
     * @param minD
     *            (minimun accepted number of units)
     * @param seedCount
     *            Number of seeds to use (the greater the number, the best but
     *            it will take longer to compute)
     * @param pivotable
     *            A pivotable object that tells which objects should be added as
     *            pivots and which should not.
     */
    public TentaclePivotSelectorShort(short minD, int seedCount,
            Pivotable < O > pivotable) {
        super(pivotable);
        this.minD = minD;
        this.seedCount = seedCount;
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
        int i = 0;
        ArrayList<DResult> result = new ArrayList<DResult>(seedCount);
        while (i < seedCount) {
            DResult temp = obtainDAux(x);
            result.add(temp);
            i++;
        }
        Object[] arr = result.toArray();
        Arrays.sort(arr);
        // Note: I have experimentally confirmed that taking
        // the median will return *much* better results than
        // taking the rightmost element of this array.
        DResult res = (DResult)arr[(int)(seedCount * 0.50)];// take the median object
        
        d = (short) res.getD();
        logger.debug("Selecting distance: " + d);
        return res.getObj();
    }

    protected DResult obtainDAux(AbstractPivotIndex < O > x)
            throws InstantiationException, IllegalAccessException,
            DatabaseException, OBException {
        OBRandom r = new OBRandom();
        int m = x.getMaxId();
        int id;
        O ob;
        do {
            id = r.nextInt(m);
            ob = x.getObject(id);
        } while (!pivotable.canBeUsedAsPivot(ob));
        int i = 0;
        MightyStaticBin1D data = new MightyStaticBin1D(true, false, 4);
        while (i < m) {
            if (id != i) {
                O temp = x.getObject(i);
                short res = ob.distance(temp);
                if (logger.isDebugEnabled()) {
                    if (i % 100000 == 0) {
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

        logger.debug("D found by harmonic mean: " + mean);
        return new DResult(ob, mean);
    }

    @Override
    protected boolean withinRange(O a, O b) throws OBException {
        return Math.abs(a.distance(b) - d) <= minD;
    }

    /**
     * Utility class used to return an OB and its harmonic distance with all the
     * objects of the database.
     * @author amuller
     */
    private class DResult implements Comparable <DResult>{
        /**
         * Object to be returned
         */
        private O obj;

        /**
         * Mean distance of the object and all the elements in the database.
         */
        private double d;

        public DResult(O obj, double d) {
            super();
            this.obj = obj;
            this.d = d;
        }

        public double getD() {
            return d;
        }

        public void setD(double d) {
            this.d = d;
        }

        public O getObj() {
            return obj;
        }

        public void setObj(O obj) {
            this.obj = obj;
        }
        
        public int compareTo(DResult r){
            if( d < r.d ){
                return -1;
            }else if(d == r.d){
                return 0;
            }else{
                return 1;
            }
        }

    }

}
