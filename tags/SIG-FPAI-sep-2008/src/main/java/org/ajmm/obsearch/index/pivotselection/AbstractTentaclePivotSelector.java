package org.ajmm.obsearch.index.pivotselection;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.PivotSelector;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.   
 */
/**
 * This pivot selector first finds a random element D, gets the average distance
 * d of the element and all the objects in the database. Then it will find
 * random pivots that are all at least d units away of each other. The algorithm
 * might have to try with a smaller d if there are no pivots that satisfy the
 * previous condition.
 * @param <O>
 *            Type of object of the index to be analyzed.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public abstract class AbstractTentaclePivotSelector < O extends OB > implements
        PivotSelector < O > {

    protected Pivotable < O > pivotable;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractTentaclePivotSelector.class);

    /**
     * Generates n (n = pivots) from the database. The method will modify the
     * pivot index and update the information of the selected new pivots
     * @param x
     *            Generate pivots from this index
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws PivotsUnavailableException
     *             If the pivots could not be found
     */
    public void generatePivots(final AbstractPivotIndex < O > x)
            throws OBException, IllegalAccessException, InstantiationException,
            DatabaseException, PivotsUnavailableException {
        O prev = obtainD(x);
        int m = x.getMaxId();
        short pivotsCount = x.getPivotsCount();
        int[] res = new int[pivotsCount];
        // ugly but welcome to java generics
        O[] obs = x.emptyPivotsArray();
        boolean repeat = true;
        while (repeat) {
            int id = 0;
            int i = 0;
            while (id < m && i < pivotsCount) {
                O current = x.getObject(id);
                // only take a guy that can be used as a pivot
                if (pivotable.canBeUsedAsPivot(current)) {
                    if ((i == 0 && withinRange(prev, current))
                            || withinRangeAll(current, obs, i)) {
                        // if we are processing the first element, we check it
                        // against
                        // the "seed" object found by obtainD
                        // otherwise we verify that current preserves
                        // withinRange
                        // with all the other elements
                        obs[i] = current;
                        res[i] = id;
                        i++;
                    }
                }
                id++;
            }
            if (i == pivotsCount) {
                repeat = false; // we are done
            } else {
                logger.debug("Repeating, reached until id:" + id + " pivot # " + i);
                if (!easifyD()) {
                    throw new PivotsUnavailableException();
                }
            }
        }
        x.storePivots(res);
    }

    /**
     * Decreases the current value of D (makes it potentially easier to find
     * pivots to satisfy).
     * @return true if easification was succesful
     */
    protected abstract boolean easifyD();

    /**
     * Return true if the given object (current) is withinRange from 0 to i - 1
     * @param current
     *            object to process
     * @param obs
     *            pivot candidates
     * @param i
     *            process the pivots up to i
     * @return true if the given object is within range
     * @throws OBException
     *             User generated exception
     */
    protected boolean withinRangeAll(final O current, final O[] obs, final int i)
            throws OBException {
        int cx = 0;
        while (cx < i) {
            if (!withinRange(current, obs[cx])) {
                return false;
            }
            cx++;
        }
        return true;
    }

    /**
     * Finds D for the given index. Stores D somewhere so that the method
     * withinRange can work properly
     * @param x
     *            The index that will be processed
     * @return The object used as "seed"
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    protected abstract O obtainD(AbstractPivotIndex < O > x)
            throws InstantiationException, IllegalAccessException,
            DatabaseException, OBException;

    /**
     * Returns true if a and b are within range.
     * @param a
     *            object
     * @param b
     *            object
     * @return true if the distance between the objects is greater or equal than
     *         the minimum distance accepted.
     * @throws OBException
     *             User generated exception
     */
    protected abstract boolean withinRange(O a, O b) throws OBException;

    /**
     * Constructor.
     * @param pivotable
     *            A pivotable object that tells which objects should be added as
     *            pivots and which should not.
     */
    public AbstractTentaclePivotSelector(Pivotable < O > pivotable) {
        super();
        this.pivotable = pivotable;
    }
}
