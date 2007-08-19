package org.ajmm.obsearch.asserts;

import org.ajmm.obsearch.exception.OBException;

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
 * This class contains utility functions that help to check for conditions and
 * create exceptions if those conditions are not met. The functions can be
 * statically imported for convenience
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public final class OBAsserts {

    /**
     * An utility class doesn't have constructors.
     */
    private OBAsserts() {

    }

    /**
     * Creates an OBException with the given msg if the given condition is
     * false.
     * @param condition
     *            Condition to be tested
     * @param msg
     *            Msg to output in the exception if condition == false
     * @throws OBException
     *             If an error occurrs it will be wrapped in an OBException
     */
    public static void chkAssert(final boolean condition, final String msg)
            throws OBException {
        if (!condition) {
            throw new OBException(msg);
        }
    }

    /**
     * Creates an IllegalArgumentException with the given msg if the given
     * condition is false.
     * @param condition
     *            Condition to be tested
     * @param msg
     *            Msg to output in the exception if condition == false
     */
    public static void chkParam(final boolean condition, final String msg) {
        if (!condition) {
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Checks that the given value "toCheck" is within range [min, max]. If not,
     * a IndexOutOfBoundsException is thrown.
     * @param toCheck
     *            Value to be checked
     * @param min
     *            minimum bound
     * @param max
     *            maximum bound
     * @throws IndexOutOfBoundsException
     */
    public static void chkRange(final int toCheck, final int min, final int max) {
        if (toCheck < min || toCheck > max) {
            throw new IndexOutOfBoundsException("Value: " + toCheck
                    + " out of range: [" + min + ", " + max + "]");
        }
    }

    /**
     * Checks that the given value "toCheck" is within range [min, max]. If not,
     * a IndexOutOfBoundsException is thrown.
     * @param toCheck
     *            Value to be checked
     * @param min
     *            minimum bound
     * @param max
     *            maximum bound
     */
    public static void chkRange(final long toCheck, final long min,
            final long max) {
        if (toCheck < min || toCheck > max) {
            throw new IndexOutOfBoundsException("Value: " + toCheck
                    + " out of range: [" + min + ", " + max + "]");
        }
    }

    /**
     * Checks that the given value "toCheck" is within range [min, max]. If not,
     * a IndexOutOfBoundsException is thrown.
     * @param toCheck
     *            Value to be checked
     * @param min
     *            minimum bound
     * @param max
     *            maximum bound
     */
    public static void chkRange(final short toCheck, final short min,
            final short max) {
        if (toCheck < min || toCheck > max) {
            throw new IndexOutOfBoundsException("Value: " + toCheck
                    + " out of range: [" + min + ", " + max + "]");
        }
    }

}
