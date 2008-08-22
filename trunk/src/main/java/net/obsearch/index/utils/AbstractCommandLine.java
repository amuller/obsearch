package net.obsearch.index.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.ambient.Ambient;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.freehep.util.argv.BooleanOption;
import org.freehep.util.argv.DoubleOption;
import org.freehep.util.argv.IntOption;
import org.freehep.util.argv.StringOption;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.sleepycat.je.DatabaseException;

public abstract class AbstractCommandLine<O extends OB, I extends Index<O>, A extends Ambient<O, I>> {

	private static Logger logger = Logger.getLogger(AbstractCommandLine.class);

	/**
	 * Properties that influence this application.
	 */
	protected Properties props;

	enum Mode {
		search, create, add
	};

	@Option(name = "-h", usage = "Print help message", aliases = { "--help" })
	private boolean help = false;

	@Option(name = "-v", usage = "Print version information", aliases = { "--version" })
	private boolean version = false;

	@Option(name = "-db", usage = "Database Folder. Path to the folder where the DB is located", aliases = { "--database" })
	private File databaseFolder;

	@Option(name = "-l", usage = "Load data into the DB. (only in create mode)", aliases = { "--load" })
	private File load;

	@Option(name = "-p", usage = "# of pivots to be employed. Used in create mode only", aliases = { "--pivots" })
	protected int pivots = 7;

	@Option(name = "-k", usage = "# of closest objects to be retrieved.")
	protected int k = 1;
	

	@Option(name = "-m", usage = "Set the mode in search, create(start a new DB), add (add data to an existing database)", aliases = { "--mode" })
	private Mode mode;

	@Option(name = "-q", usage = "Query Filename. (Search mode only)", aliases = { "--query" })
	private File query;
	
	@Option(name = "-b", usage = "If bulk mode is to be employed", aliases= {"--bulk"})
	protected boolean bulkMode;

	public void initProperties() throws IOException {

		InputStream is = this.getClass().getResourceAsStream(
				File.separator + "application.properties");
		props = new Properties();
		props.load(is);
		// configure log4j only once too
		String prop = props.getProperty("log4j.file");
		PropertyConfigurator.configure(prop);
	}
	
	/**
	 * Return the "this" reference, used to access all the
	 * command line options.
	 * @return The reference of the bottommost class that contains parameters.
	 */
	protected abstract AbstractCommandLine getReference();

	/**
	 * This method must be called by the sons of this class.
	 * 
	 * @param thisReference
	 *            this reference of the subclass.
	 * @param args
	 *            Arguments sent to the application.
	 */
	public void processUserCommands(String args[]){
		
		try {
			initProperties();
		} catch (final Exception e) {
			System.err.print("Make sure log4j is configured properly"
					+ e.getMessage());
			e.printStackTrace();
			System.exit(48);
		}
		
		CmdLineParser parser = new CmdLineParser(getReference());
		try {
			parser.parseArgument(args);
			// arguments have been loaded.
			if(help){
				parser.printUsage(System.err);
			}
			switch(mode) {
				case create: create(); return;
				case search: search(); return;
				case add: add(); return;
			}

		}catch( CmdLineException e ) {
			logger.fatal("Error in command line arguments", e);
			parser.printUsage(System.err);
			System.err.println();
			System.exit(32);
		} catch (Exception e) {
			logger.fatal(e);
			e.printStackTrace();
			System.exit(33);
		}

	}

	protected abstract A instantiateNewAmbient(File dbFolder)
			throws OBStorageException, OBException, FileNotFoundException,
			IllegalAccessException, InstantiationException, IOException;

	protected abstract A instantiateAmbient(File dbFolder)
			throws OBStorageException, OBException, FileNotFoundException,
			IllegalAccessException, InstantiationException, IOException;

	/**
	 * Adds objects to the index. Loads the objects from File.
	 * @param index Index to load the objects into.
	 * @param load File to load.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws OBStorageException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected abstract void addObjects(I index, File load) throws FileNotFoundException, IOException, OBStorageException, OBException, IllegalAccessException, InstantiationException;
	
	protected abstract void searchObjects(I index, File load) throws IOException, OBException, InstantiationException, IllegalAccessException;

	protected void create() throws IOException, OBStorageException,
			OBException, DatabaseException, InstantiationException,
			IllegalAccessException {
		OBAsserts.chkFileNotExists(databaseFolder);
		OBAsserts.chkFileExists(load);

		A ambiente = instantiateNewAmbient(databaseFolder);
		I index = ambiente.getIndex();

		logger.info("Loading Data...");
		addObjects(index, load);

		ambiente.freeze();

		logger.info(index.getStats());
		ambiente.close();
	}

	protected void add() throws IOException, OBStorageException, OBException,
			DatabaseException, InstantiationException, IllegalAccessException {
		OBAsserts.chkFileExists(databaseFolder);
		OBAsserts.chkFileExists(load);
		
		A ambiente = instantiateAmbient(databaseFolder);		
		I index = ambiente.getIndex();

		logger.info("Loading Data...");
		addObjects(index, load);
		
		logger.info(index.getStats());
		ambiente.close();
	}
	
	protected void search() throws IOException, OBStorageException, OBException,
	DatabaseException, InstantiationException, IllegalAccessException {
		OBAsserts.chkFileExists(databaseFolder);
		OBAsserts.chkFileExists(query);
		A ambiente = instantiateAmbient(databaseFolder);		
		I index = ambiente.getIndex();

		logger.info("Searching... ");
		searchObjects(index, query);
		
		logger.info(index.getStats());
		ambiente.close();
	}

}
