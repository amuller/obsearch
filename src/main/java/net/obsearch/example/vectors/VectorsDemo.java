package net.obsearch.example.vectors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.log4j.PropertyConfigurator;

import net.obsearch.ambient.Ambient;
import net.obsearch.ambient.bdb.AmbientBDBJe;

import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.idistance.impl.IDistanceIndexInt;
import net.obsearch.index.idistance.impl.IDistanceIndexShort;
import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.TUtils;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezInt;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.result.OBPriorityQueueInt;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultInt;
import net.obsearch.storage.bdb.Utils;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2009 Arnoldo Jose Muller Molina

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

/**
 * VectorsDemo shows how to use OBSearch in vector spaces (L1 distance). 
 * 
 * @author Arnoldo Jose Muller Molina
 */

public class VectorsDemo {
	
	/**
	 * Dimension of the vectors.
	 */
	final static int VEC_SIZE = 100;
	
	/**
	 * Database size.
	 */
	final static int DB_SIZE = 20000000;
	
	/**
	 * Query count.
	 */
	final static int QUERY_SIZE = 100;
		
	/**
	 * Index folder
	 */
	
	final static File INDEX_FOLDER = new File("." + File.separator + "index");
	
	
	final static Random r = new Random();
	
	/**
	 * Logging provided by Java
	 */
	static Logger logger = Logger.getLogger(VectorsDemo.class.getName());
	
	/**
	 * Randomly generate a vector.
	 * @return a randomly generated vector.
	 */
	public static L1 generateVector(){				
		
		short[] data = new short[VEC_SIZE];		
		int i = 0;
				
		while(i < data.length){
			data[i] = (short)r.nextInt(500);
			i++;
		}
		
		return new L1(data);
	}
	
	public static void init() throws IOException{
		
		InputStream is = VectorsDemo.class.getResourceAsStream(
				File.separator + "obsearch.properties");
		Properties props = new Properties();
		props.load(is);
		String prop = props.getProperty("log4j.file");
		PropertyConfigurator.configure(prop);
	}

	public static void main(String args[]) throws FileNotFoundException, OBStorageException, NotFrozenException, IllegalAccessException, InstantiationException, OBException, IOException {
				
		init();
		
		// Create a pivot selection strategy for L1 distance
		IncrementalBustosNavarroChavezShort<L1> sel = new IncrementalBustosNavarroChavezShort<L1>(
				new AcceptAll<L1>(), 5000, 1000);

		// Create the iDistance method with 126 pivots
		IDistanceIndexShort<L1> index = new IDistanceIndexShort<L1>(L1.class, sel, 64);

		// Delete the directory of the index just in case.
		Directory.deleteDirectory(INDEX_FOLDER);

		// Create the ambient that will store the index's data. (NOTE: folder name is hardcoded)
		Ambient<L1, IDistanceIndexShort<L1>> a =  new AmbientBDBJe<L1, IDistanceIndexShort<L1>>( index, INDEX_FOLDER );
		
		
		// Add some random objects to the index:	
		logger.info("Adding " + DB_SIZE + " objects...");
		int i = 0;		
		while(i < DB_SIZE){
			index.insert(generateVector());
			i++;
		}
		
		// prepare the index
		logger.info("Preparing the index...");
		a.freeze();
		
		// now we can match some objects!		
		logger.info("Querying the index...");
		i = 0;
		index.resetStats(); // reset the stats counter
		long start = System.currentTimeMillis();
		while(i < QUERY_SIZE){
			L1 q = 	generateVector();	
			// query the index with k=1			
			OBPriorityQueueShort<L1> queue = new OBPriorityQueueShort<L1>(1);			
			// perform a query with r=3000000 and k = 1 
			index.searchOB(q, Short.MAX_VALUE, queue);
			// you can see the results with this loop:
			/*Iterator<OBResultInt<L1>> it =  queue.iterator();
			while(it.hasNext()){
				OBResultInt<L1> res = it.next(); 
				L1 answerObject = res.getObject(); // get the answer object
				long id = res.getId(); // the id of the answer object
				int distance = res.getDistance(); // the distance of the object to the query
			}*/
			i++;
		}
		// print the results of the set of queries. 
		long elapsed = System.currentTimeMillis() - start;
		logger.info("Time per query: " + elapsed / QUERY_SIZE + " millisec.");
		
		logger.info("Stats follow: (total distances / pivot vectors computed during the experiment)");
		logger.info(index.getStats().toString());


	}

}
