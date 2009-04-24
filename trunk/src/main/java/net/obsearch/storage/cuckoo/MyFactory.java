package net.obsearch.storage.cuckoo;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.utils.Directory;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreByte;
import net.obsearch.storage.OBStoreDouble;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreFloat;
import net.obsearch.storage.OBStoreInt;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.OBStoreShort;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.OBStorageConfig.IndexType;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * Factory that creates MyFactory* objects.
 * @author Arnoldo Jose Muller-Molina
 *
 */
public class MyFactory implements OBStoreFactory {

	private File directory;
	
	
	
	public MyFactory(File directory) {
		super();
		this.directory = directory;
	}

	@Override
	public void close() throws OBStorageException {
		// TODO Auto-generated method stub

	}

	/**
	 * Create a storage device with 
	 */
	@Override
	public OBStore<TupleBytes> createOBStore(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		File location = createFile(name);
		try{
			HashFunction f1 = (HashFunction)Class.forName(OBSearchProperties.getStringProperty("my.hash.f1")).newInstance();
			HashFunction f2 = (HashFunction)Class.forName(OBSearchProperties.getStringProperty("my.hash.f2")).newInstance();
			OBAsserts.chkNotNull(f1, "F1 hash function");
			OBAsserts.chkNotNull(f2, "F2 hash function");
		MyStorage s = new MyStorage(null, new CuckooHash(OBSearchProperties.getLongProperty("my.expected.db.count"), location, f1, f2 ), name, this);
		return s;
		}catch(Exception e){
			throw new OBStorageException(e);
		}
	}
	
	private File createFile(String name) throws OBStorageException{
		File location = new File(directory, name);
		location.mkdirs();
		OBAsserts.chkFileExists(location);
		return location;
	}

	@Override
	public OBStoreByte createOBStoreByte(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OBStoreDouble createOBStoreDouble(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OBStoreFloat createOBStoreFloat(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OBStoreInt createOBStoreInt(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OBStoreLong createOBStoreLong(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		File location = createFile(name);
		ByteArray index;
		if(config.getIndexType() == IndexType.FIXED_RECORD){
			index = new ByteArrayFixed(config.getRecordSize(), location );
		}else{
			index =  new ByteArrayFlex(location);
		}
		
		try{
						
		MyStorageLong s = new MyStorageLong(index, null, name, this);
		return s;
		}catch(Exception e){
			throw new OBStorageException(e);
		}
		
	}

	@Override
	public OBStoreShort createOBStoreShort(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigInteger deSerializeBigInteger(byte[] value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte deSerializeByte(byte[] value) {		
		return ByteConversion.bytesToByte(value);
	}

	@Override
	public double deSerializeDouble(byte[] value) {
		return ByteConversion.bytesToDouble(value);
	}

	@Override
	public float deSerializeFloat(byte[] value) {
		return ByteConversion.bytesToFloat(value);
	}

	@Override
	public int deSerializeInt(byte[] value) {		
		return ByteConversion.bytesToInt(value);	
	}

	@Override
	public long deSerializeLong(byte[] value) {
		return ByteConversion.bytesToLong(value);	
	}

	@Override
	public short deSerializeShort(byte[] value) {
		return ByteConversion.bytesToShort(value);
	}

	@Override
	public String getFactoryLocation() {
		return directory.getAbsolutePath();
	}

	@Override
	public void removeOBStore(OBStore storage) throws OBStorageException,
			OBException {
		storage.deleteAll();
		File dir = new File(directory, storage.getName());
		try{
		Directory.deleteDirectory(dir);
		}catch(IOException e){
			throw new OBStorageException(e);
		}
	}

	@Override
	public byte[] serializeBigInteger(BigInteger value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] serializeByte(byte value) {
		return ByteConversion.byteToBytes(value);
	}

	@Override
	public byte[] serializeDouble(double value) {
		
		return ByteConversion.doubleToBytes(value);
	}

	@Override
	public byte[] serializeFloat(float value) {

		return ByteConversion.floatToBytes(value);
	}

	@Override
	public byte[] serializeInt(int value) {

		return ByteConversion.intToBytes(value);
	}

	@Override
	public byte[] serializeLong(long value) {
		return ByteConversion.longToBytes(value);
	}

	@Override
	public byte[] serializeShort(short value) {
		return ByteConversion.shortToBytes(value);
	}

	@Override
	public Object stats() throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

}
