package net.obsearch.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

public class TestAesaFloat2 {
	protected static Logger logger = Logger.getLogger(TestAesaFloat.class
			.getName());
	

	private int DIM = 5;
	private int DB_SIZE = 2000;
	private int QUERY_SIZE = 100;
	
	public List<OBTanimoto> create2(File f, int size) throws IOException, OBException{
		List<OBTanimoto> res = new ArrayList<OBTanimoto>();
		BufferedReader data = new BufferedReader(new FileReader(f));
		String line = data.readLine();
		int i = 0;
		while(i < size && line != null ){
			res.add(new OBTanimoto(line));
			line = data.readLine();
			i++;
		}
		return res;
		
	}
	
	@Test
	public void testAesa2() throws OBException, IllegalAccessException, InstantiationException, IOException{
		
		List<OBTanimoto> db = create2(new File("/home/amuller/wikipedia/hope.txt.db"), DB_SIZE);
		List<OBTanimoto> query = create2(new File("/home/amuller/wikipedia/hope.txt.q"), QUERY_SIZE);
		
		AesaFloat<OBTanimoto> a = new AesaFloat<OBTanimoto>(OBTanimoto.class, DB_SIZE);
		for(OBTanimoto d : db){
			a.insert(d);
		}
		a.freeze();
		
		List<OBQueryFloat> queries = new ArrayList<OBQueryFloat>(QUERY_SIZE);
		for(OBTanimoto q : query){
			OBQueryFloat<OBTanimoto> search = new OBQueryFloat<OBTanimoto>(q, Float.MAX_VALUE, new OBPriorityQueueFloat<OBTanimoto>(1));
			queries.add(search);
			a.searchOB(search);		
			
		}
		logger.info("Dist: " + a.getStatistics().getDistanceCount());
		logger.info(a.getStatistics().toString());
		IndexUtilsFloat.validateResults(queries, a);
	}
}
