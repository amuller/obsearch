package net.obsearch.index.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.apache.log4j.Logger;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.ambient.Ambient;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;

public abstract class AbstractNewLineCommandLine<O extends OB, I extends Index<O>, A extends Ambient<O, I>> extends AbstractCommandLine<O, I, A> {

	private static Logger logger = Logger.getLogger(AbstractNewLineCommandLine.class);
	
	private BufferedReader createReader(File toOpen) throws FileNotFoundException{
		
		return new BufferedReader(new InputStreamReader(new FileInputStream(toOpen),  Charset.forName("US-ASCII")));
	}
	
	protected void addObjects(I index, File load) throws IOException, OBStorageException, OBException, IllegalAccessException, InstantiationException{
		BufferedReader r = createReader(load);
		String line = r.readLine();
		int i = 0;
		while(line != null){
			O o = instantiate(line);
			index.insert(o);
			line = r.readLine();
			if(i % 10000 == 0){
				logger.info("Loading: " + i);
			}
			i++;
		}
	}
	
	protected void searchObjects(I index, File load) throws IOException, OBException, InstantiationException, IllegalAccessException{
		BufferedReader r = createReader(load);
		String line = r.readLine();
		index.resetStats();
		int i = 0;
		logger.info("Searching with r: " + r + " k: " + k);
		while(line != null && i < super.maxQueries){
			O o = instantiate(line);
			queries++;
			searchObject(index, o);
			if(i % 100 == 0){
				logger.info("Searching: " + i);
			}
			
			i++;
			line = r.readLine();
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
	 */
	protected abstract void searchObject(I index, O object) throws NotFrozenException, IllegalIdException, OutOfRangeException, InstantiationException, IllegalAccessException, OBException;

	
	/**
	 * Instantiate an object from a string.
	 * @return The object
	 * @throws OBException 
	 */
	protected abstract O instantiate(String line) throws OBException;
	
}
