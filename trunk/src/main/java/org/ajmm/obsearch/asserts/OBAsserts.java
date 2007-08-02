package org.ajmm.obsearch.asserts;

import org.ajmm.obsearch.exception.OBException;

/**
 * This class contains utility functions that help to check for conditions and
 * create exceptions if those conditions are not met. The functions can be
 * statically imported for convenience
 */
public class OBAsserts {

	/**
	 * Creates an OBException with the given msg if the given condition is false
	 * 
	 * @param condition
	 *            Condition to be tested
	 * @param msg
	 *            Msg to output in the exception if condition == false
	 * @throws OBException
	 */
	public final static void chkAssert(boolean condition, String msg)
			throws OBException {
		if (!condition) {
			throw new OBException(msg);
		}
	}

	/**
	 * Creates an IllegalArgumentException with the given msg if the given
	 * condition is false
	 * 
	 * @param condition
	 *            Condition to be tested
	 * @param msg
	 *            Msg to output in the exception if condition == false
	 * @throws IllegalArgumentException
	 */
	public final static void chkParam(boolean condition, String msg)
			throws IllegalArgumentException {
		if (!condition) {
			throw new IllegalArgumentException(msg);
		}
	}

	/**
	 * Checks that the given value "toCheck" is within range [min, max]
	 * 
	 * @param toCheck
	 *            Value to be checked
	 * @param min
	 *            minimum bound
	 * @param max
	 *            maximum bound
	 * @throws IndexOutOfBoundsException
	 */
	public static void chkRange(int toCheck, int min, int max)
			throws IndexOutOfBoundsException {
		if (toCheck < min || toCheck > max) {
			throw new IndexOutOfBoundsException("Value: " + toCheck
					+ " out of range: [" + min + ", " + max + "]");
		}
	}

	/**
	 * Checks that the given value "toCheck" is within range [min, max]
	 * 
	 * @param toCheck
	 *            Value to be checked
	 * @param min
	 *            minimum bound
	 * @param max
	 *            maximum bound
	 * @throws IndexOutOfBoundsException
	 */
	public static void chkRange(long toCheck, long min, long max)
			throws IndexOutOfBoundsException {
		if (toCheck < min || toCheck > max) {
			throw new IndexOutOfBoundsException("Value: " + toCheck
					+ " out of range: [" + min + ", " + max + "]");
		}
	}

	/**
	 * Checks that the given value "toCheck" is within range [min, max]
	 * 
	 * @param toCheck
	 *            Value to be checked
	 * @param min
	 *            minimum bound
	 * @param max
	 *            maximum bound
	 * @throws IndexOutOfBoundsException
	 */
	public static void chkRange(short toCheck, short min, short max)
			throws IndexOutOfBoundsException {
		if (toCheck < min || toCheck > max) {
			throw new IndexOutOfBoundsException("Value: " + toCheck
					+ " out of range: [" + min + ", " + max + "]");
		}
	}

}
