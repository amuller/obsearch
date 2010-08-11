package net.obsearch.asserts;

import java.io.File;
import java.io.IOException;

import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;

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
 * 
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
	 * 
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
	
	public static void chkNotNull(Object x, String msg) throws OBException{
		if(x == null){
			throw new OBException("Object cannot be null: " + msg);
		}
	}

	public static void chkAssertStorage(final boolean condition, final String msg)
			throws OBStorageException {
		if (!condition) {
			throw new OBStorageException(msg);
		}
	}

	/**
	 * Creates an IllegalArgumentException with the given msg if the given
	 * condition is false.
	 * 
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

	public static void notNull(Object x) throws OBException {
		if (x == null) {
			throw new OBException("Null objects are invalid");
		}
	}

	/**
	 * Checks that the given file exists, otherwise throws an IOException
	 * 
	 * @throws IOException
	 *             if the given file does not exist
	 * @param file
	 *            File that will be confirmed for existence
	 */
	public static void chkFileExists(final File file) throws OBStorageException {
		if (!file.exists()) {
			throw new OBStorageException("File:" + file + "does not exist");
		}
	}

	public static void chkFileExists(String file) throws OBStorageException {
		chkFileExists(new File(file));
	}

	public static void chkFileNotExists(String file) throws OBStorageException {
		chkFileNotExists(new File(file));
	}

	public static void chkFileNotExists(File file) throws OBStorageException {
		if (file.exists()) {
			throw new OBStorageException("File:" + file + "exists and it should not.");
		}
	}

	/**
	 * Fail with the given message.
	 * 
	 * @param msg
	 *            The message to be thrown.
	 */
	public static void fail(String msg) throws OBException {
		throw new OBException(msg);
	}

	/**
	 * Checks that the given value "toCheck" is within range [min, max]. If not,
	 * a IndexOutOfBoundsException is thrown.
	 * 
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
	 * 
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
	 * Checks if the given number is greater or equal than 0.
	 * 
	 * @param x
	 */
	public static void chkPositive(final long x) {
		if (!(x >= 0)) {
			throw new IllegalArgumentException(x
					+ " is not greater or equal than 0");
		}
	}

	/**
	 * Make sure x is even.
	 * 
	 * @param x
	 *            number to verify
	 * @return true if x is even
	 */
	public static void chkEven(long x) {
		if ((x % 2) != 0) {
			throw new IllegalArgumentException(x
					+ " is not even and it should be even");
		}
	}

	/**
	 * Checks that the given value "toCheck" is within range [min, max]. If not,
	 * a IndexOutOfBoundsException is thrown.
	 * 
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
