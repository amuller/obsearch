package net.obsearch.storage.tc;

import tokyocabinet.DBM;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;

public class TCOBStorageBytesArray extends AbstractTCOBStorage<TupleBytes> {
	
	
	/**
     * Builds a new Storage system by receiving a Berkeley DB database.
     * @param db
     *                The database to be stored.
     * @param name
     *                Name of the database.
     * @param fact the factory of the storage device
     * @param duplicates if duplicates are allowed in this storage
     * @throws DatabaseException
     *                 if something goes wrong with the database.
     */
	
	public TCOBStorageBytesArray(String name, DBM db, OBStoreFactory fact,
			 OBStorageConfig conf) throws OBStorageException, OBException {
		super(name,db,fact, conf);
	}
																		
    

	@Override
    public CloseIterator < TupleBytes > processAll() throws OBStorageException {
        return new ByteArrayIterator();
    }



	@Override
	public byte[] prepareBytes(byte[] in) {
		return in;
	}
	
	

}
