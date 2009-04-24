package net.obsearch.storage.cuckoo;

import java.io.IOException;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;

public class MyStorageLong extends AbstractMyStorage implements OBStoreLong {
	
	public MyStorageLong(ByteArray adb,  CuckooHash hdb,
			String name, OBStoreFactory fact) throws OBException {
		super(adb,hdb,name,fact);
	}

	@Override
	public long bytesToValue(byte[] entry) {
		return fact.deSerializeLong(entry);
	}
	
	/**
	 * Verify that we are actually using the byte array.
	 * @throws OBException 
	 */
	private void checkByteArray() throws OBException{
		OBAsserts.chkAssert(aDB != null, "You must use a ByteArray for my long storage devices");

	}

	@Override
	public OperationStatus delete(long key) throws OBStorageException  {
		OperationStatus res = new OperationStatus();
		try{
		if(aDB.delete(key)){
			res.setStatus(Status.EXISTS);
		}else{			
			res.setStatus(Status.NOT_EXISTS);
		}
		}catch(Exception e){
			throw new OBStorageException(e);
		}
		return res;
	}

	@Override
	public byte[] getValue(long key) throws IllegalArgumentException,
			OBStorageException {		
		try {
			return aDB.get(key);
		}  catch (Exception e) {
			throw new OBStorageException(e);
		}
	}

	@Override
	public CloseIterator<TupleLong> processRange(long low, long high)
			throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CloseIterator<TupleLong> processRangeReverse(long low, long high)
			throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationStatus put(long key, byte[] value)
			throws OBStorageException {
		OperationStatus res = new OperationStatus();
		try {
			aDB.put(key, value);
			res.setStatus(Status.OK);
			return res;
		} catch (Exception e) {
			throw new OBStorageException(e);
		}
		
	}
	
	

	@Override
	public CloseIterator<TupleLong> processAll() throws OBStorageException {
		return aDB.iterator();
	}

}
