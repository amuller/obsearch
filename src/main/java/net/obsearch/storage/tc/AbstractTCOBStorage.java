package net.obsearch.storage.tc;

/*
 * OBSearch: a distributed similarity search engine This project is to
 * similarity search what 'bit-torrent' is to downloads. Copyright (C) 2008
 * Arnoldo Jose Muller Molina
 * 
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

import net.obsearch.storage.OBStore;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Arrays;

import tokyocabinet.BDB;
import tokyocabinet.DBM;
import tokyocabinet.FDB;
import tokyocabinet.HDB;

import net.obsearch.Index;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.utils.ByteArrayComparator;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.Tuple;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteConversion;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStorageConfig.IndexType;

public abstract class AbstractTCOBStorage<T extends Tuple> implements
		OBStore<T> {

	protected StaticBin1D stats = new StaticBin1D();

	/**
	 * Tokyo cabinet database
	 */
	private DBM db;

	/**
	 * Name of the database.
	 */
	private String name;

	/**
	 * Factory of this storage device.
	 */
	protected OBStoreFactory fact;

	/**
	 * Sequences are stored in this record.
	 */
	private final int SEQUENCE_RECORD = 0;

	/**
	 * METADATA info.
	 */
	private FDB metadata;

	private long currentId = 0;

	/**
	 * Only one cursor operation allowed at once.
	 */
	private boolean doingCursor = false;

	private OBStorageConfig storageConf;

	/**
	 * Builds a new Storage system by receiving a Berkeley DB database.
	 * 
	 * @param db
	 *            The database to be stored.
	 * @param name
	 *            Name of the database.
	 * @param sequences
	 *            Database used to store sequences.
	 * @throws OBException
	 * @throws OBStorageException
	 * @throws DatabaseException
	 *             if something goes wrong with the database.
	 */
	public AbstractTCOBStorage(String name, DBM db, OBStoreFactory fact,
			OBStorageConfig storageConf) throws OBStorageException, OBException {
		this.db = db;
		this.name = name;
		this.fact = fact;
		OBStorageConfig conf = new OBStorageConfig();
		conf.setFixedSizeIndex(true);
		conf.setRecordSize(Index.ID_SIZE);
		metadata = new FDB();
		metadata.open(fact.getFactoryLocation() + File.separator + name
				+ "_META", FDB.OWRITER | FDB.OCREAT | FDB.OREADER);
		currentId = getSequence();
		this.storageConf = storageConf;
	}

	/**
	 * Return the current sequence id.
	 * 
	 * @return the current sequence id.
	 */
	private long getSequence() {
		byte[] rec = metadata.get(fact
				.serializeInt(SEQUENCE_RECORD));
		if(rec == null){
			return 0;
		}else{
			return fact.deSerializeLong(rec);
		}
		
	}

	private void putSequence(long id) {
		metadata
				.put(fact.serializeInt(SEQUENCE_RECORD), fact.serializeLong(id));
	}

	public OBStoreFactory getFactory() {
		return fact;
	}

	public void close() throws OBStorageException {
		boolean res;
		String code;
		if (db instanceof FDB) {
			FDB dbx = (FDB) db;
			res = dbx.close();
			code = lastErrorString();
		} else if (db instanceof HDB) {
			res = ((HDB) db).close();
			code = lastErrorString();
		} else if (db instanceof BDB) {
			res = ((BDB) db).close();
			code = lastErrorString();
		} else {
			throw new OBStorageException("Invalid method");
		}
		if (!res) {
			throw new OBStorageException("Unable to close the database: "
					+ code);
		}
		this.putSequence(currentId);
		OBAsserts.chkAssertStorage(metadata.close(), "Could not close metadata DB");
	}

	private int lastErrorCode() {
		int res = Integer.MIN_VALUE;
		if (db instanceof FDB) {
			res = ((FDB) db).ecode();
		} else if (db instanceof HDB) {
			res = ((HDB) db).ecode();
		} else if (db instanceof BDB) {
			res = ((BDB) db).ecode();
		}
		return res;
	}

	private String lastErrorString() {
		String res = "";
		if (db instanceof FDB) {
			res = ((FDB) db).errmsg();
		} else if (db instanceof HDB) {
			res = ((HDB) db).errmsg();
		} else if (db instanceof BDB) {
			res = ((BDB) db).errmsg();
		}
		return res;
	}

	public net.obsearch.OperationStatus delete(byte[] key)
			throws OBStorageException {
		net.obsearch.OperationStatus r = new net.obsearch.OperationStatus();
		if (db.out(key)) {
			r.setStatus(Status.OK);
		} else {
			r.setStatus(Status.NOT_EXISTS);
			r.setMsg(this.lastErrorString());
		}
		return r;
	}

	public void deleteAll() throws OBStorageException {

		boolean res = false;
		if (db instanceof FDB) {
			res = ((FDB) db).vanish();
		} else if (db instanceof HDB) {
			res = ((HDB) db).vanish();
		} else if (db instanceof BDB) {
			res = ((BDB) db).vanish();
		}

		if (!res) {
			throw new OBStorageException("Could not truncate the database: "
					+ this.lastErrorString());
		}
	}

	public String getName() {
		return this.name;
	}

	public byte[] getValue(byte[] key) throws IllegalArgumentException,
			OBStorageException {
		if (storageConf.isDuplicates()) {
			throw new IllegalArgumentException();
		}

		byte[] value = db.get(key);
		return value;

	}

	public net.obsearch.OperationStatus put(byte[] key, byte[] value)
			throws OBStorageException {
		OBAsserts
				.chkAssertStorage(
						storageConf.getIndexType() != IndexType.FIXED_RECORD
								|| (storageConf.getIndexType() == IndexType.FIXED_RECORD && value
										.length == storageConf
										.getRecordSize()),
						"Record size does not match the size for this index");
		net.obsearch.OperationStatus res = new net.obsearch.OperationStatus();
		if (db.put(key, value)) {
			res.setStatus(Status.OK);
		} else {
			res.setMsg(this.lastErrorString());
		}
		return res;
	}

	public boolean allowsDuplicatedData() {
		return storageConf.isDuplicates();
	}

	public CloseIterator<TupleBytes> processRange(byte[] low, byte[] high)
			throws OBStorageException {
		throw new IllegalArgumentException();
	}

	public CloseIterator<TupleBytes> processRangeNoDup(byte[] low, byte[] high)
			throws OBStorageException {
		throw new IllegalArgumentException();
	}

	public CloseIterator<TupleBytes> processRangeReverse(byte[] low, byte[] high)
			throws OBStorageException {
		throw new IllegalArgumentException();
	}

	public CloseIterator<TupleBytes> processRangeReverseNoDup(byte[] low,
			byte[] high) throws OBStorageException {
		throw new IllegalArgumentException();
	}
	
	public CloseIterator < byte[] > processAllKeys() throws OBStorageException {
        return new ByteArrayKeyIterator();
    }

	/**
	 * Base class used to iterate over cursors. Only supports full search
	 * 
	 * @param <O>
	 *            The type of tuple that will be returned by the iterator.
	 */
	protected abstract class CursorIterator<T> implements CloseIterator<T> {

		protected byte[] nextKey;

		protected byte[] nextValue;

		protected byte[] lastReturnedKey;

		protected CursorIterator() throws OBStorageException {
			if (doingCursor) {
				throw new OBStorageException("Only one cursor at a time");
			}
			doingCursor = true;
			db.iterinit();

			loadNext();

		}

		public boolean hasNext() {
			return nextKey != null;
		}

		/**
		 * Loads data from keyEntry and dataEntry and puts it into next. If we
		 * go beyond max, we set next to null so that everybody will work
		 * properly.
		 */
		protected void loadNext() throws NoSuchElementException {
			nextKey = db.iternext();
			if(nextKey != null){
				nextValue = db.get(nextKey);
			}else{
				// stop the cursor.
				doingCursor = false;
			}
		}

		protected T createT(byte[] key, byte[] value) {
			return createTuple(key, value);
		}

		/**
		 * Creates a tuple from the given key and value.
		 * 
		 * @param key
		 *            raw key.
		 * @param value
		 *            raw value.
		 * @return A new tuple of type T created from the raw data key and
		 *         value.
		 */
		protected abstract T createTuple(byte[] key, byte[] value);

		public T next() {
			T res = createT(nextKey, nextValue);
			lastReturnedKey = nextKey;
			loadNext();
			return res;

		}

		public void closeCursor() throws OBException {
			doingCursor = false;
		}

		public void remove() {
			if (lastReturnedKey != null) {
				db.out(lastReturnedKey);
			}

		}

	}

	public long size() throws OBStorageException {

		long res = db.rnum();

		return res;

	}

	/**
	 * Returns the next id from the database (incrementing sequences).
	 * 
	 * @return The next id that can be inserted.
	 */
	public long nextId() throws OBStorageException {
		synchronized (metadata) {
			long res = currentId;
			this.currentId++;			
			return res;
		}
	}

	@Override
	public StaticBin1D getReadStats() {
		return this.stats;
	}

	@Override
	public void setReadStats(StaticBin1D stats) {
		this.stats = stats;
	}

	protected class ByteArrayIterator extends CursorIterator<TupleBytes> {

		protected ByteArrayIterator() throws OBStorageException {
			super();
		}

		@Override
		protected TupleBytes createTuple(byte[] key, byte[] value) {
			return new TupleBytes(key, value);
		}
	}
	
	protected class ByteArrayKeyIterator extends CursorIterator<byte[]> {

		protected ByteArrayKeyIterator() throws OBStorageException {
			super();
		}

		@Override
		protected byte[] createTuple(byte[] key, byte[] value) {
			return key;
		}
		
		protected void loadNext() throws NoSuchElementException {
			nextKey = db.iternext();			
		}

	}

}
