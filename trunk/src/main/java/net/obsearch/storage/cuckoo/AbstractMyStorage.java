package net.obsearch.storage.cuckoo;

import java.io.IOException;

import hep.aida.bin.StaticBin1D;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.utils.OBFactory;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;

/**
 * This storage device is a Java native driver composed of the following
 * subsystems: - Cuckoo hashing (for the OBStore<TupleBytes>) interface -
 * ByteArrayFixed for fixed size records (better with the OBStore<TupleLong>) -
 * ByteArrayFlex for variable size records (better with the OBStore<TupleLong>)
 * 
 * @author amuller
 * 
 */
public abstract class AbstractMyStorage  {

	/**
	 * Cuckoo hash used in the most general case.
	 */
	protected CuckooHash hdb;

	/**
	 * byte[] array database
	 */
	protected ByteArray aDB;

	/**
	 * Name of the device.
	 */
	private String name;

	/**
	 * Factory of the storage device.
	 */
	protected OBStoreFactory fact;
	
	
	

	public AbstractMyStorage(ByteArray adb,  CuckooHash hdb,
			String name, OBStoreFactory fact) throws OBException {
		super();
		aDB = adb;
		this.fact = fact;
		this.hdb = hdb;
		this.name = name;
		//checkKeyType();
	}

	
	public void close() throws OBStorageException {
		try {
			if (hdb != null) {
				hdb.close();
			}
			if (aDB != null) {
				aDB.close();
			}
		} catch (Exception e) {
			throw new OBStorageException(e);
		}

	}

	/**
	 * If we receive an array key, we should be using only the hash table.
	 * 
	 * @throws OBException
	 */
	private void checkKeyType() throws OBException {
		OBAsserts
				.chkAssert(hdb != null && aDB == null,
						"If you use byte arrays as keys, you should use the hash table");
	}


	public OperationStatus delete(byte[] key) throws IOException, OBException {
		checkKeyType();
		boolean res = hdb.delete(key);
		OperationStatus result = new OperationStatus();
		if (res) {
			result.setStatus(Status.EXISTS);
		} else {
			result.setStatus(Status.NOT_EXISTS);
		}
		return result;
	}

	
	public void deleteAll() throws OBStorageException {
		try {
			if (hdb != null) {
				hdb.deleteAll();
			}
			if (aDB != null) {
				aDB.deleteAll();
			}
		} catch (IOException e) {
			throw new OBStorageException(e);
		}
	}

	
	public OBStoreFactory getFactory() {
		return fact;
	}

	
	public String getName() {
		return name;
	}

	
	public StaticBin1D getReadStats() {
		return null;
	}

	
	public byte[] getValue(byte[] key) throws IllegalArgumentException,
			 OBException {
		checkKeyType();
		try{
		byte[] res = hdb.get(key);
		return res;
		}catch(IOException e){
			throw new OBException(e);
		}
		
	}

	
	public long nextId() throws OBStorageException {
		return size();
	}

	
	public void optimize() throws OBStorageException {
		// TODO Auto-generated method stub

	}

	
	public byte[] prepareBytes(byte[] in) {
		return in;
	}

	
	public Object getStats() throws  OBException{
		Object res = null;
		try{
			if(hdb != null){
				res = hdb.getStats();
			}
		}catch(IOException e){
			throw new OBStorageException(e);
		}
		return res;
	}

	
	
	public CloseIterator<TupleBytes> processRange(byte[] low, byte[] high)
			throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CloseIterator<TupleBytes> processRangeNoDup(byte[] low, byte[] high)
			throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CloseIterator<TupleBytes> processRangeReverse(byte[] low, byte[] high)
			throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public CloseIterator<TupleBytes> processRangeReverseNoDup(byte[] low,
			byte[] high) throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	
	public OperationStatus put(byte[] key, byte[] value)
			throws OBException {
		checkKeyType();
		try{
			hdb.put(key, value);
			OperationStatus res = new OperationStatus();
			res.setStatus(Status.OK);
			return res;
		}catch(IOException e){
			throw new OBStorageException(e);
		}
	}

	
	

	
	public void setReadStats(StaticBin1D stats) {
		// TODO Auto-generated method stub

	}

	
	public long size() throws OBStorageException {
		try {
			if (hdb != null) {
				return hdb.size();
			} else {
				return aDB.size();
			}
		} catch (IOException e) {
			throw new OBStorageException(e);
		}
	}

}
