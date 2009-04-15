package net.obsearch.storage;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.logging.Logger;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.OBStorageConfig.IndexType;
import net.obsearch.storage.bdb.Utils;
import net.obsearch.storage.tc.TCFactory;
import net.obsearch.utils.bytes.ByteConversion;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestTCByteArrayStorage {
	

	private static final transient Logger logger = Logger
		.getLogger(TestTCByteArrayStorage.class.getCanonicalName());

	private static int DB_SIZE = 10000;

	@Test
	public void testByteArrayStorage() throws Exception {
		OBStorageConfig config = new OBStorageConfig();
		config.setRecordSize(8);
		for(IndexType i : IndexType.values()){
			if(i != IndexType.FIXED_RECORD){
				config.setIndexType(i);
				validateStorageWithConfig(config);
			}
		}				
	}
	
	private void addObjects(OBStore<TupleBytes> st, HashMap<Long, Double> data,TCFactory tc ) throws OBStorageException{
		for (Map.Entry<Long, Double> e : data.entrySet()) {
			OperationStatus res = st.put(tc.serializeLong(e.getKey()),
					(tc.serializeDouble(e
							.getValue())));
			assertTrue(res.getMsg(), res.getStatus() == Status.OK);
			// repeating the operation introduces an OK.
			res = st.put(tc.serializeLong(e.getKey()), (tc.serializeDouble(e.getValue())));
			assertTrue(res.getStatus() == Status.OK);
		}
		assertTrue(data.size() == st.size());
	}


	protected void validateStorageWithConfig(OBStorageConfig config)
			throws Exception {
		logger.info("Doing index: " + config.getIndexType() );
		TCFactory tc = Utils.getFactoryTC();
		OBStore<TupleBytes> st = tc.createOBStore("test", config);
		HashMap<Long, Double> data = new HashMap<Long, Double>();
		// create some random data.
		int i = 0;
		Random r = new Random();
		while (i < DB_SIZE) {
			data.put((long) r.nextInt(DB_SIZE * 1000), r.nextDouble());
			i++;
		}
		
		addObjects(st,data,tc);
		
		// check if the data exists
		for (Map.Entry<Long, Double> e : data.entrySet()) {
			byte[] val = st.getValue(tc.serializeLong(e.getKey()));
			assertTrue(tc.deSerializeDouble(val) == e.getValue());
		}

		// delete the data and make sure it disappears

		for (Map.Entry<Long, Double> e : data.entrySet()) {
			OperationStatus res = st.delete(tc.serializeLong(e.getKey()));
			assertTrue(res.getStatus() == Status.OK);

			byte[] val = st.getValue(tc.serializeLong(e.getKey()));
			assertTrue(val == null); // nothing exists.
			// doing the operation again will say that the data does not exist.
			res = st.delete(tc.serializeLong(e.getKey()));
			assertTrue(res.getStatus() == Status.NOT_EXISTS);
		}
		
		// add the objects again and do an iteration.
		addObjects(st,data,tc);
		
		// make sure all the keys are there.
		CloseIterator<byte[]> itk = st.processAllKeys();
		HashSet<Long> keySet = new HashSet<Long>();
		while(itk.hasNext()){
			byte[] k = itk.next();
			long key = tc.deSerializeLong(k);
			assertTrue(data.containsKey(key));
			keySet.add(key);
		}
		itk.closeCursor();
		assertTrue(data.size() == keySet.size());
		
		CloseIterator<TupleBytes> it = st.processAll();
		while(it.hasNext()){
			TupleBytes t = it.next();
			assertTrue(data.remove(tc.deSerializeLong(t.getKey())) == tc.deSerializeDouble( t.getValue()));			
		}
		it.closeCursor();
		assertTrue(data.size() == 0);
		st.close();
	}

}
