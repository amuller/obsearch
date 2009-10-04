package net.obsearch.example;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.Test;

import net.obsearch.example.doc.OBTanimoto;
import net.obsearch.exception.OBException;
import net.obsearch.index.IndexUtilsFloat;
import net.obsearch.index.OBVectorFloat;
import net.obsearch.index.aesa.impl.AesaFloat;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.result.OBPriorityQueueFloat;

public class TestAesaFloat {
	protected static Logger logger = Logger.getLogger(TestAesaFloat.class
			.getName());
	

	private int DIM = 5;
	private int DB_SIZE = 2000;
	private int QUERY_SIZE = 100;
	@Test
	public void testAesa() throws OBException, IllegalAccessException, InstantiationException{
		
		List<OBVectorFloat> db = create(DB_SIZE);
		List<OBVectorFloat> query = create(QUERY_SIZE);
		
		AesaFloat<OBVectorFloat> a = new AesaFloat<OBVectorFloat>(DB_SIZE);
		for(OBVectorFloat d : db){
			a.insert(d);
		}
		a.freeze();
		
		List<OBQueryFloat> queries = new ArrayList<OBQueryFloat>(QUERY_SIZE);
		for(OBVectorFloat q : query){
			OBQueryFloat<OBVectorFloat> search = new OBQueryFloat<OBVectorFloat>(q, Float.MAX_VALUE, new OBPriorityQueueFloat<OBVectorFloat>(1));
			queries.add(search);
			a.searchOB(search);		
			break;
		}
		logger.info("Dist: " + a.getStatistics().getDistanceCount());
		logger.info(a.getStatistics().toString());
		IndexUtilsFloat.validateResults(queries, a);
	}

	public List<OBVectorFloat> create(int size) {
		Random r = new Random();
		List<OBVectorFloat> result = new ArrayList<OBVectorFloat>(size);
		int i = 0;
		while (i < size) {
			result.add(new OBVectorFloat(r, DIM));
			i++;
		}
		return result;
	}

	
	
}
