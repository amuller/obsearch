package net.obsearch.index;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCacheHandlerLong;
import net.obsearch.cache.OBCacheLong;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.OBStore;
import net.obsearch.utils.bytes.ByteConversion;

import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreLong;

import com.thoughtworks.xstream.XStream;

/**
 * AbstractOBIndex contains functionality regarding object storage. Children of
 * this class should define a way of searching those objects. This class
 * provides default implementations of methods that are considered optional in
 * the Index interface.
 * 
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractOBIndex<O extends OB> implements Index<O> {

	/**
	 * If we should auto generate the id. true if we call first
	 * {@link #insert(OB)}.
	 */
	private boolean autoGenerateId = true;
	/**
	 * Used to detect the first call of an insert method.
	 */
	private boolean firstInsert = true;

	/**
	 * Statistics related to this index.
	 */
	protected transient Statistics stats;

	/**
	 * Objects are stored by their id's here.
	 */
	protected transient OBStoreLong A;

	/**
	 * Factory used by this class and by subclasses to create appropiate storage
	 * devices.
	 */
	protected transient OBStoreFactory fact;

	/**
	 * Required during pre-freeze to make only one copy of an object is
	 * inserted. (the index is not built at this stage therefore it is not
	 * possible to know if an object is already in the DB.
	 */
	private transient OBStore preFreeze;

	/**
	 * If we should use the pre-freeze db. Use this if you don't know if the
	 * before-freeze data is totally unique. Setting this to false will speed up
	 * a lot insertion before freeze. You should turn this flag off specially if
	 * the data objects (OBs) are big. TODO: use an md5 sum to check if objects
	 * are in preFreeze. It would make things much more efficient and cheap for
	 * prefreeze.
	 */
	private boolean isPreFreeze = true;

	/**
	 * Cache used for storing recently accessed objects O.
	 */
	private transient OBCacheLong<O> aCache;

	/**
	 * True if this index is frozen.
	 */
	private boolean isFrozen;

	/**
	 * The type used to instantiate objects of type O.
	 */
	private Class<O> type;

	/**
	 * Constructors of an AbstractOBIndex should receive only parameters related
	 * to the operation of the index. The factory and the initialization will be
	 * executed by {@link #init(OBStoreFactory)}
	 * 
	 * @param type
	 * @throws OBStorageException
	 * @throws OBException
	 */
	protected AbstractOBIndex(Class<O> type) throws OBStorageException,
			OBException {
		this.type = type;
	}

	/**
	 * Returns the type of the object to be stored.
	 * 
	 * @return {@link #type}
	 */
	protected final Class<O> getType() {
		return type;
	}

	/**
	 * If the database is frozen returns silently if it is not throws
	 * NotFrozenException.
	 * 
	 * @throws NotFrozenException
	 *             if the index has not been frozen.
	 */
	protected void assertFrozen() throws NotFrozenException {
		if (!isFrozen()) {
			throw new NotFrozenException();
		}
	}

	/**
	 * Initialize the index.
	 * 
	 * @throws OBStorageException
	 * @throws OBException
	 */
	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, NotFrozenException, IllegalAccessException,
			InstantiationException, OBException {
		this.fact = fact;
		initStorageDevices();
		initCache();
		stats = new Statistics();
	}

	/**
	 * Initializes storage devices required by this class.
	 * 
	 * @throws OBStorageException
	 *             If the storage device could not be created.
	 */
	protected void initStorageDevices() throws OBStorageException {
		this.A = fact.createOBStoreLong("A", false, false);
		if (!this.isFrozen()) {
			this.preFreeze = fact.createOBStore("pre", true, false);
		}
	}

	/**
	 * Initializes the object cache {@link #aCache}.
	 * 
	 * @throws DatabaseException
	 *             If something goes wrong with the DB.
	 */
	protected void initCache() throws OBException {
		aCache = new OBCacheLong<O>(new ALoader(), OBSearchProperties
				.getACacheSize());
	}

	/**
	 * This class is in charge of loading objects.
	 * 
	 * @author amuller
	 */
	private class ALoader implements OBCacheHandlerLong<O> {

		public long getDBSize() throws OBStorageException {
			return A.size();
		}

		public O loadObject(long i) throws OBException, InstantiationException,
				IllegalAccessException, IllegalIdException {

			ByteBuffer data = A.getValue(i);
			if (data == null) {
				throw new IllegalIdException(i);
			}

			return bytesToObject(data.array());
		}

		@Override
		public void store(long key, O object) throws OBException {
			// nothing to do, we already store A when we should.
		}

	}

	/**
	 * If we are going to check for the existence of data before freezing.
	 * 
	 * @return true if we are going to check for the existence of data before
	 *         freezing.
	 */
	public boolean isPreFreeze() {
		return isPreFreeze;
	}

	/**
	 * If we are going to check for the existence of data before freezing
	 * isPreFreeze should be set to true. If you know each object is unique then
	 * you can set this to false and with this improve performance.
	 * 
	 * @param isPreFreeze
	 *            true (quality, data integrity) false (performance)
	 */
	public void setPreFreeze(boolean isPreFreeze) {
		this.isPreFreeze = isPreFreeze;
	}

	/**
	 * Instantiates an object O from the given data array.
	 */
	protected O bytesToObject(ByteBuffer data) throws OBException,
			InstantiationException, IllegalAccessException, IllegalIdException {
		return bytesToObject(data.array());
	}

	/**
	 * Instantiates an object O from the given data array.
	 */
	protected O bytesToObject(byte[] data) throws OBException,
			InstantiationException, IllegalAccessException, IllegalIdException {
		O res = type.newInstance();
		try {
			res.load(data);
		} catch (IOException e) {
			throw new OBException(e);
		}
		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#close()
	 */
	@Override
	public void close() throws OBException {
		A.close();
		// if (this.preFreeze != null) {
		// preFreeze.close();
		// }
		fact.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#databaseSize()
	 */
	@Override
	public long databaseSize() throws OBStorageException {
		return A.size();
	}

	public String debug(O object) throws OBException, InstantiationException,
			IllegalAccessException {
		return object.toString();
	}

	@Override
	public OperationStatus delete(O object) throws OBException,
			IllegalAccessException, InstantiationException, NotFrozenException {
		if (this.isFrozen()) {
			OperationStatus res = deleteAux(object);
			if (res.getStatus() == Status.OK) {
				this.A.delete(res.getId());
				assert A.getValue(res.getId()) == null;
			}
			return res;
		} else {
			throw new NotFrozenException();
		}
	}

	/**
	 * Deletes the entry of this object in the index. The current class will
	 * remove the object from A if the result status is
	 * {@link net.obsearch.Status#OK}.
	 * 
	 * @param object
	 *            object to be deleted.
	 * @return {@link net.obsearch.Status#OK} if the object was deleted.
	 *         {@link net.obsearch.Status#NOT_EXISTS} no object matched.
	 */
	protected abstract OperationStatus deleteAux(O object) throws OBException,
			IllegalAccessException, InstantiationException;

	/**
	 * Converts an object into an array of bytes.
	 * 
	 * @param object
	 *            Object to convert.
	 * @return The bytes array representation of the object.
	 */
	protected byte[] objectToBytes(O object) throws OBException {
		try {
			return object.store();

		} catch (IOException e) {
			throw new OBException(e);
		}
	}

	protected ByteBuffer objectToByteBuffer(O object) throws OBException {
		return ByteConversion.createByteBuffer(objectToBytes(object));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#getObject(long)
	 */
	@Override
	public O getObject(long id) throws IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {
		// get the object from A, this is easy.
		return aCache.get(id);
	}

	/**
	 * Find the Id of the given object. (the distance 0 is considered as equal)
	 * 
	 * @param object
	 *            The object to search
	 * @return {@link net.obsearch.Status#OK} if the object is found (with the
	 *         id) otherwise, {@link net.obsearch.Status#NOT_EXISTS}
	 * @throws IllegalIdException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws OBException
	 */
	protected OperationStatus findAux(O object) throws IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#exists(net.obsearch.result.OB)
	 */

	/*
	 * public OperationStatus exists(O object) throws OBStorageException,
	 * OBException, IllegalAccessException, InstantiationException {
	 * OperationStatus t = findAux(object); if (t.getStatus() == Status.OK) {
	 * t.setStatus(Status.EXISTS); } return t; }
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#getStats()
	 */
	@Override
	public Statistics getStats() throws OBStorageException {
		stats.putStats("Read Stats A", A.getReadStats());
		stats.putObjects("Env", fact.stats());
		return stats;
	}

	public void setIdAutoGeneration(boolean auto) throws OBException {
		OBAsserts.chkAssert(A.size() == 0,
				"Cannot change id generation if the index is not empty");
		autoGenerateId = auto;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#insert(net.obsearch.result.OB)
	 */
	@Override
	public OperationStatus insert(O object) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException {
		return insert(object, -1);

	}

	public OperationStatus insertBulk(O object) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException {
		return insertBulk(object, A.nextId());
	}

	public OperationStatus insertBulk(O object, long id)
			throws OBStorageException, OBException, IllegalAccessException,
			InstantiationException {

		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);

		// validate if the id is not in the DB.
		res.setId(id);
		if (this.isFrozen()) {

			// must insert object into A before the index is updated
			OBAsserts.chkAssert(A.getValue(id) == null,
					"id already used, fatal error");
			this.A.put(id, objectToByteBuffer(object));
			// update the index:
			
			res = insertAux(id, object);
			

		} else { // before freeze
					
			OBAsserts.chkAssert(A.getValue(id) == null,
					"id already used, fatal error");
			this.A.put(id, objectToByteBuffer(object));

		}
		return res;
	}

	public OperationStatus insert(O object, long id) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException {

		OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);

		// validate if the id is not in the DB.

		if (this.isFrozen()) {
			res = exists(object);
			if (res.getStatus() == Status.NOT_EXISTS) {
				// must insert object into A before the index is updated
				OBAsserts.chkAssert(A.getValue(id) == null,
						"id already used, fatal error");
				this.A.put(id, objectToByteBuffer(object));
				// update the index:
				if (id == -1) {
					id = A.nextId();
				}
				res = insertAux(id, object);
				res.setId(id);
			}

		} else { // before freeze
			// we keep track of objects that have been inserted
			// based on their binary signature.
			// TODO: maybe change this to a hash to avoid the problem
			// with objects that have multiplicity.
			if (isPreFreeze) {
				byte[] key = objectToBytes(object);
				ByteBuffer value = this.preFreeze.getValue(key);
				if (value == null) {
					if (id == -1) {
						id = A.nextId();
					}
					res.setId(id);
					preFreeze.put(key, ByteConversion.longToByteBuffer(id));
				} else {
					res.setStatus(Status.EXISTS);
					res.setId(ByteConversion.byteBufferToLong(value));
				}
			} else {
				res.setId(id);
			}

			// insert the object in A if everything is OK.
			if (res.getStatus() == Status.OK) {
				if (id == -1) {
					id = A.nextId();
					res.setId(id);
				}
				OBAsserts.chkAssert(A.getValue(id) == null,
						"id already used, fatal error");
				this.A.put(res.getId(), objectToByteBuffer(object));
			}

		}
		return res;
	}

	/**
	 * Inserts the given object into the particular index. The caller inserts
	 * the actual object so the implementing class only has to worry about
	 * adding the id inside the index.
	 * 
	 * @param id
	 *            The id that will be used to insert the object.
	 * @param object
	 *            The object that will be inserted.
	 * @return If {@link net.obsearch.Status#OK} or
	 *         {@link net.obsearch.Status#EXISTS} then the result will hold the
	 *         id of the inserted object and the operation is successful.
	 *         Otherwise an exception will be thrown.
	 * @throws OBStorageException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected abstract OperationStatus insertAux(long id, O object)
			throws OBStorageException, OBException, IllegalAccessException,
			InstantiationException;

	/**
	 * Inserts the given object into the particular index. No checks regarding
	 * existence are performed. We assume the user already checked uniqueness.
	 * 
	 * @param id
	 *            The id that will be used to insert the object.
	 * @param object
	 *            The object that will be inserted.
	 * @return {@link net.obsearch.Status#OK}
	 * 
	 * @throws OBStorageException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected abstract OperationStatus insertAuxBulk(long id, O object)
			throws OBStorageException, OBException, IllegalAccessException,
			InstantiationException;

	/**
	 * @see net.obsearch.Index#freeze()
	 */
	@Override
	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OBStorageException, OutOfRangeException, OBException {
		if (isFrozen()) {
			// TODO: allow indexes to freeze multiple times.
			throw new AlreadyFrozenException();
		}
		this.isFrozen = true;
		preFreeze.close(); // only during freeze.
		// fact.removeOBStore(preFreeze);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#getBox(net.obsearch.result.OB)
	 */
	@Override
	public long getBox(O object) throws OBException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#isFrozen()
	 */
	@Override
	public boolean isFrozen() {
		return this.isFrozen;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#resetStats()
	 */
	@Override
	public void resetStats() {
		stats.resetStats();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.result.Index#totalBoxes()
	 */
	@Override
	public long totalBoxes() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Finds the given objects in A and serializes them.
	 * 
	 * @param ids
	 *            Objects that will be loaded
	 * @return Serialization of the objects.
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws IllegalIdException
	 */
	protected byte[][] serializePivots(final long[] ids)
			throws IllegalIdException, IllegalAccessException,
			InstantiationException, OBException {
		byte[][] result = new byte[ids.length][];
		int i = 0;
		while (i < ids.length) {
			O obj = getObject(ids[i]);
			result[i] = this.objectToBytes(obj);

			i++;
		}
		return result;
	}

	/**
	 * Create an empty pivots array.
	 * 
	 * @return an empty pivots array of size {@link #pivotsCount}.
	 */
	public O[] emptyPivotsArray(int size) {
		return (O[]) Array.newInstance(this.getType(), size);
	}

	protected O[] loadPivots(byte[][] serializedPivots)
			throws IllegalIdException, OBException, InstantiationException,
			IllegalAccessException {
		O[] result = emptyPivotsArray(serializedPivots.length);
		int i = 0;
		while (i < serializedPivots.length) {
			result[i] = bytesToObject(serializedPivots[i]);
			i++;
		}
		assert i == serializedPivots.length; // pivot count and read # of pivots
		return result;
	}

}
