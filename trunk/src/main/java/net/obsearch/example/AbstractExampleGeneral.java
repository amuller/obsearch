package net.obsearch.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.logging.Logger;

import net.obsearch.asserts.OBAsserts;

import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.utils.Directory;

import org.apache.log4j.PropertyConfigurator;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import cern.colt.Arrays;

public abstract class AbstractExampleGeneral {

	/**
	 * Folder where the index will be stored
	 */
	@Option(name = "-db", usage = "Database Folder. Path to the folder where the DB is located", aliases = { "--database" })
	protected File indexFolder = null;
	@Option(name = "-dbsrc", usage = "Database source file", aliases = { "--database_source" })
	protected File database = null;
	@Option(name = "-qsrc", usage = "Query source file", aliases = { "--query_source" })
	protected File query = null;
	/**
	 * Database max size
	 */
	@Option(name = "-dbsize", usage = "Size of the DB", aliases = { "--database_size" })
	protected int databaseSize = 10000000;
	/**
	 * Query count.
	 */
	@Option(name = "-query_size", usage = "Number of queries to execute", aliases = { "--query_size" })
	protected int querySize = 2000;
	/**
	 * Help message
	 */
	@Option(name = "-h", usage = "Print help message", aliases = { "--help" })
	protected boolean help = false;
	
	
	
	

	/**
	 * things the program can do
	 * 
	 */
	protected enum Mode {
		search, // search data
		create, // create db
		intrinsic
		// intrinsic dim
	}

	/**
	 * Program mode
	 */
	@Option(name = "-m", usage = "Set the program execution mode", aliases = { "--mode" })
	protected Mode mode;
	/**
	 * Logging provided by Java
	 */
	protected static Logger logger = Logger.getLogger(AbstractExampleGeneral.class
			.getName());

	public void init() throws IOException {

		InputStream is = AbstractExampleGeneral.class.getResourceAsStream(File.separator
				+ "obsearch.properties");
		Properties props = new Properties();
		props.load(is);
		String prop = props.getProperty("log4j.file");
		PropertyConfigurator.configure(prop);
	}

	public AbstractExampleGeneral(String args[]) throws IOException,
			OBStorageException, OBException, IllegalAccessException,
			InstantiationException, PivotsUnavailableException {
		super();
		init();
		doIt(args);
	}

	
	protected abstract void create() throws FileNotFoundException, OBStorageException, OBException, IOException, IllegalAccessException, InstantiationException, PivotsUnavailableException;

	protected abstract void search() throws FileNotFoundException, OBStorageException, NotFrozenException, IllegalAccessException, InstantiationException, OBException, IOException;
	
	protected abstract void intrinsic() throws IllegalIdException, IllegalAccessException, InstantiationException, OBException, FileNotFoundException, IOException;
	
	public void doIt(String args[]) throws IOException, OBStorageException,
			OBException, IllegalAccessException, InstantiationException,
			PivotsUnavailableException {

		CmdLineParser parser = new CmdLineParser(this);

		try {
			parser.parseArgument(args);
			// arguments have been loaded.
			if (help) {
				parser.printUsage(System.err);
			}
		} catch (Exception e) {

			e.printStackTrace();

		}


		if (mode == Mode.create) {
			

			logger.info("Creating index in : " + indexFolder);			

			// Delete the directory of the index just in case.
			Directory.deleteDirectory(indexFolder);
			OBAsserts.chkAssert(indexFolder.mkdirs(), "Could not create index folder");
			
			create();
			

		} else if (mode == Mode.search) {

			search();
			
		} else if (mode == Mode.intrinsic) {
			
			intrinsic();
		}
	}
	
	
	protected int read(byte[] buffer, InputStreamReader r) throws IOException {
		int i = 0;
		while (i < buffer.length) {
			int b = r.read();
			if (b == -1 && i != 0) {
				throw new IOException(
						"Reached end of file before we could complete one read");
			} else if (b == -1) {
				return b; // we are done.
			}
			assert b >= 0;
			assert b <= Byte.MAX_VALUE;
			buffer[i] = (byte) b;
			i++;
		}
		// we should have a newline here
		int b = r.read();
		if (b == -1) {
			return b;
		} else if (b != '\n') {
			throw new IOException("Format is incorrect: " + b + " line read:" + Arrays.toString(buffer) + " buffer size: " + buffer.length);
		}
		return b;
	}
}