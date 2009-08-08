package net.obsearch.storage.tc;

import java.io.File;
import java.math.BigInteger;

import tokyocabinet.BDB;
import tokyocabinet.DBM;
import tokyocabinet.FDB;
import tokyocabinet.HDB;
import tokyocabinet.Util;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
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
import net.obsearch.storage.bdb.BDBFactoryJe;
import net.obsearch.utils.bytes.ByteConversion;

public class TCFactory implements OBStoreFactory {
	/**
	 * Directory where all the objects will be created.
	 */
	private String directory;
	
	public TCFactory(File directory){
		this.directory = directory.getAbsolutePath();
	}

	@Override
	public void close() throws OBStorageException {

	}

	private DBM createDB(String name, OBStorageConfig config) throws OBStorageException, OBException{
		DBM db = null;
		String path = directory + File.separator + name;
		if (IndexType.HASH == config.getIndexType()  ) {
			HDB tdb = new HDB();
			OBAsserts.chkAssertStorage(tdb.tune((long) (OBSearchProperties.getLongProperty("tc.expected.db.count") * 4), -1, 20, HDB.TLARGE ), "Could not set the tuning parameters for the hash table: " + tdb.errmsg() );
			OBAsserts.chkAssertStorage(tdb.setcache(200000), "Could not enable cache size");
			OBAsserts.chkAssertStorage(tdb.setxmsiz((long)Math.pow(1024, 2) * 500), "Could not enable mmap  size");
			OBAsserts.chkAssertStorage(tdb.open(path, HDB.OCREAT |  HDB.OWRITER | HDB.ONOLCK), "Could not open database: " + tdb.errmsg());			
			db = tdb;
		} else if (IndexType.BTREE == config.getIndexType() || IndexType.DEFAULT == config.getIndexType()) {
			BDB tdb = new BDB();
			tdb.tune(-1, -1, OBSearchProperties.getLongProperty("tc.expected.db.count") / 128, -1, -1, BDB.TLARGE);
			OBAsserts.chkAssertStorage(tdb.open(path, BDB.OCREAT |  BDB.OWRITER | BDB.ONOLCK), "Could not open database: " + tdb.errmsg() );			
			db = tdb;
		} else if(IndexType.FIXED_RECORD == config.getIndexType()){
			FDB tdb = new FDB();
			OBAsserts.chkAssert(config.getRecordSize() > 0, "Invalid record size");
			OBAsserts.chkAssertStorage(tdb.tune(config.getRecordSize(), OBSearchProperties.getLongProperty("tc.fdb.max.file.size")), "Could not set the tuning parameters for the fixed record device");
			OBAsserts.chkAssertStorage(tdb.open(path, FDB.OCREAT |  FDB.OWRITER | FDB.ONOLCK), "Could not open database: " + tdb.errmsg() );
			db = tdb;
		}else{
			OBAsserts.fail("Fatal error, invalid index type.");
		}
		return db;
	}
	
	@Override
	public OBStore<TupleBytes> createOBStore(String name, OBStorageConfig config)
			throws OBStorageException, OBException {

		DBM db = createDB(name, config);
		TCOBStorageBytesArray t = new TCOBStorageBytesArray(name, db, this, config);
		return t;
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
		DBM db = createDB(name, config);
		TCOBStorageLong t = new TCOBStorageLong(name, db, this, config);
		return t;
	}

	@Override
	public OBStoreShort createOBStoreShort(String name, OBStorageConfig config)
			throws OBStorageException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigInteger deSerializeBigInteger(byte[] value) {		
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
		//return ByteConversion.bytesToInt(value);
		return Util.unpackint(value);
	}

	@Override
	public long deSerializeLong(byte[] value) {
		//return ByteConversion.bytesToLong(value);
		//return BDBFactoryJe.bytesToLong(value);
		return Long.parseLong(new String(value));
	}

	@Override
	public short deSerializeShort(byte[] value) {
		return ByteConversion.bytesToShort(value);
	}

	@Override
	public String getFactoryLocation() {
		return this.directory;
	}

	@Override
	public void removeOBStore(OBStore storage) throws OBStorageException, OBException {
		storage.deleteAll();
		storage.close();
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
		//return ByteConversion.intToBytes(value);
		return Util.packint(value);
	}

	@Override
	public byte[] serializeLong(long value) {
		//return ByteConversion.longToBytes(value);
		//return BDBFactoryJe.longToBytes(value);
		return String.valueOf(value).getBytes();
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
