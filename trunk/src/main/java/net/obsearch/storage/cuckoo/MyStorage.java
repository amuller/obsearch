package net.obsearch.storage.cuckoo;

import java.io.IOException;

import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;

public class MyStorage extends AbstractMyStorage implements OBStore<TupleBytes> {
	
	
	
	public MyStorage(ByteArray adb,  HardDiskHash hdb,
			String name, OBStoreFactory fact) throws OBException {
		super(adb,hdb,name,fact);
	}
	
	public CloseIterator<TupleBytes> processAll() throws OBException {
		try {
			return hdb.iterator();		
		}catch(IOException e){
			throw new OBStorageException(e);
		}
	}

}
