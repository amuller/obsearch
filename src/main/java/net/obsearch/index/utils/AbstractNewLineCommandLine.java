package net.obsearch.index.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.Option;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.ambient.Ambient;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.stats.Statistics;

public abstract class AbstractNewLineCommandLine<O extends OB, I extends Index<O>, A extends Ambient<O, I>> extends AbstractCommandLine<O, I, A> {

	private static Logger logger = Logger.getLogger(AbstractNewLineCommandLine.class);
	
	
	@Option(name = "-ml", usage = "# of data elements to be loaded", aliases = { "--maxLoad" })
	protected int maxLoad = Integer.MAX_VALUE;
	
	private BufferedReader createReader(File toOpen) throws FileNotFoundException{
		
		return new BufferedReader(new InputStreamReader(new FileInputStream(toOpen),  Charset.forName("US-ASCII")));
	}
	
	protected void addObjects(I index, File load) throws IOException, OBStorageException, OBException, IllegalAccessException, InstantiationException{
		BufferedReader r = createReader(load);
		String line = r.readLine();
		int i = 0;
		while(line != null && i < maxLoad){
			O o = instantiate(line);
			index.insert(o);
			line = r.readLine();
			if(i % 10000 == 0){
				logger.info("Loading: " + i);
			}
			i++;
		}
	}
	
	protected void searchObjects(I index, File load, Statistics other) throws IOException, OBException, InstantiationException, IllegalAccessException{
		BufferedReader r = createReader(load);
		String line = r.readLine();
		int i = 0;
		List<O> queries = new LinkedList<O>();
		while(line != null && i < super.maxQueries){
			O o = instantiate(line);
			queries.add(o);
			i++;
			line = r.readLine();
		}		
		r.close();
		
		i = 0;
		for(O o : queries){	
			updateParams();
			searchObject(index, o, other);
			if(i % 100 == 0){
				logger.info("Searching: " + i);
			}
			i++;			
		}		
	}
	
	/**
	 * The subclass implements this method and decides to print
	 * or do something with the result.
	 * @param index The index to be searched.
	 * @param object The object to search.
	 * @throws OBException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws OutOfRangeException 
	 * @throws IllegalIdException 
	 * @throws NotFrozenException 
	 * @throws IOException 
	 */
	protected abstract void searchObject(I index, O object, Statistics other) throws NotFrozenException, IllegalIdException, OutOfRangeException, InstantiationException, IllegalAccessException, OBException, IOException;

	/**
	 * Updates the underlying index based on different parameters
	 */
	protected abstract void updateParams();
	
	/**
	 * Instantiate an object from a string.
	 * @return The object
	 * @throws OBException 
	 */
	protected abstract O instantiate(String line) throws OBException;
	
}
