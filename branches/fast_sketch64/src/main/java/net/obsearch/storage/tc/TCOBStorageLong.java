package net.obsearch.storage.tc;

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
import java.util.Iterator;
import java.util.NoSuchElementException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleLong;

import com.sleepycat.bind.tuple.LongBinding;

import com.sleepycat.je.DatabaseEntry;

import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleInput;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import java.nio.ByteBuffer;

import tokyocabinet.DBM;
import net.obsearch.storage.OBStoreFactory;

/**
 * BDBOBStoreLong is a wrapper for Berkeley indexes that assumes that keys are
 * longs and values are byte[].
 * 
 * @author Arnoldo Jose Muller Molina
 */

public final class TCOBStorageLong extends AbstractTCOBStorage<TupleLong>
		implements OBStoreLong {

	/**
	 * Builds a new Storage system by receiving a Berkeley DB database that uses
	 * longs as a primary indexing method.
	 * 
	 * @param db
	 *            The database to be stored.
	 * @param seq
	 *            Sequences database.
	 * @param name
	 *            Name of the database.
	 * @throws OBException 
	 * @throws OBStorageException 
	 * @throws DatabaseException
	 *             if something goes wrong with the database.
	 */
	public TCOBStorageLong(String name, DBM db, OBStoreFactory fact,
			OBStorageConfig storageConf) throws OBStorageException, OBException {
		super(name, db, fact, storageConf);
	}

	public net.obsearch.OperationStatus delete(long key)
			throws OBStorageException {
		return super.delete(getBytes(key));
	}

	/**
	 * Converts the given value to an array of bytes.
	 * 
	 * @param value
	 *            the value to be converted.
	 * @return An array of bytes with the given value encoded.
	 */
	private byte[] getBytes(long value) {
		return fact.serializeLong(value);
	}

	/**
	 * Converts the value of the given entry into its primitive type.
	 * 
	 * @param entry
	 *            The place where we will put the entry.
	 */
	public long bytesToValue(byte[] entry) {
		return fact.deSerializeLong(entry);
	}

	public byte[] getValue(long key) throws IllegalArgumentException,
			OBStorageException {
		return super.getValue(getBytes(key));
	}

	public net.obsearch.OperationStatus put(long key, byte[] value)
			throws IllegalArgumentException, OBStorageException {
		return super.put(getBytes(key), value);
	}

	public CloseIterator<TupleLong> processRange(long low, long high) {
		throw new IllegalArgumentException();		
	}

	public CloseIterator<TupleLong> processRangeNoDup(long low, long high)
			throws OBStorageException {
		throw new IllegalArgumentException();
		// return new LongIterator(low, high, false,false);
	}

	public CloseIterator<TupleLong> processRangeReverse(long low, long high)
			throws OBStorageException {
		throw new IllegalArgumentException();
		// return new LongIterator(low, high,true,true);
	}

	public CloseIterator<TupleLong> processRangeReverseNoDup(long low, long high)
			throws OBStorageException {
		throw new IllegalArgumentException();
		// return new LongIterator();
	}

	public CloseIterator<TupleLong> processAll() throws OBStorageException {
		return new LongIterator();
	}

	/**
	 * Iterator used to process range results.
	 */
	/*
	 * TODO: I am leaving the closing of the cursor to the last iteration or the
	 * finalize method (whichever happens first). We should test if this is ok,
	 * or if there is an issue with this because Berkeley's iterator explicitly
	 * have a "close" method.
	 */
	final class LongIterator extends CursorIterator<TupleLong> {

		private LongIterator() throws OBStorageException {
			super();
		}

		protected TupleLong createTuple(byte[] key, byte[] value) {
			return new TupleLong(bytesToValue(key), value);
		}
	}

	@Override
	public byte[] prepareBytes(byte[] in) {
		return in;
	}
}
