package net.obsearch.index.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.ambient.Ambient;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.stats.Statistics;
import net.obsearch.utils.Pair;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.freehep.util.argv.BooleanOption;
import org.freehep.util.argv.DoubleOption;
import org.freehep.util.argv.IntOption;
import org.freehep.util.argv.StringOption;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.opt4j.benchmark.DoubleString;
import org.opt4j.core.Archive;
import org.opt4j.core.Individual;
import org.opt4j.core.Objective;
import org.opt4j.core.Objectives;
import org.opt4j.core.Objective.Sign;
import org.opt4j.core.problem.Creator;
import org.opt4j.core.problem.Evaluator;
import org.opt4j.optimizer.ea.EvolutionaryAlgorithmModule;
import org.opt4j.start.Opt4JTask;

import com.google.inject.Module;
import com.sleepycat.je.DatabaseException;

public abstract class AbstractCommandLine<O extends OB, I extends Index<O>, A extends Ambient<O, I>>
		implements Evaluator<DoubleString> {

	private static Logger logger = Logger.getLogger(AbstractCommandLine.class);

	/**
	 * Properties that modify the behavior of this application.
	 */
	protected Properties props;

	protected enum Mode {
		search, // search data
		create, // create a database
		add, // add data to an existing db.
		x, // experiment set
		// optimize an experiment set.
		opt

	};

	/**
	 * Output format
	 */
	private DecimalFormat f = new DecimalFormat("00000.0000");

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
	protected Mode mode;

	@Option(name = "-q", usage = "Query Filename. (Search mode only)", aliases = { "--query" })
	private File query;

	@Option(name = "-mq", usage = "Maximum number of queries to be executed", aliases = { "--max-queries" })
	protected int maxQueries = 1000;

	@Option(name = "-n", usage = "Name of the experiment", aliases = { "--name" })
	protected String experimentName = "default";

	@Option(name = "-rf", usage = "Experiment result filename", aliases = { "--exp-result" })
	protected String experimentResultFileName = "result.txt";

	@Option(name = "-b", usage = "Bulk mode is to be employed for create/add", aliases = { "--bulk" })
	protected boolean bulkMode = false;

	@Option(name = "-es", usage = "Experiment set, a colon separated list of  ranges and ks. Just like: r_1,k_1:r_1,k_1:...:r_n,k_n ", aliases = { "--experiment-set" })
	protected String experimentSet;

	@Option(name = "-r", usage = "Range used for retrieval")
	protected double r;

	@Option(name = "-validate", usage = "Validate results against sequential search")
	protected boolean validate = false;

	private A ambiente;

	private I index;

	/**
	 * Number of queries executed.
	 */
	protected int queries = 0;
	/**
	 * Total ellapsed time during each query.
	 */
	protected long time;

	public void initProperties() throws IOException {

		InputStream is = this.getClass().getResourceAsStream(
				File.separator + "obsearch.properties");
		props = new Properties();
		props.load(is);
		// configure log4j only once too
		String prop = props.getProperty("log4j.file");
		PropertyConfigurator.configure(prop);
	}

	/**
	 * Return the "this" reference, used to access all the command line options.
	 * 
	 * @return The reference of the bottommost class that contains parameters.
	 */
	protected abstract AbstractCommandLine getReference();

	protected I getIndex() {
		return index;
	}

	/**
	 * This method must be called by the sons of this class.
	 * 
	 * @param thisReference
	 *            this reference of the subclass.
	 * @param args
	 *            Arguments sent to the application.
	 */
	public void processUserCommands(String args[]) {

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
			if (help) {
				parser.printUsage(System.err);
			}
			switch (mode) {
			case create:
				create();
				return;
			case search:
				search();
				return;
			case add:
				add();
				return;
			case x:
				experimentSet();
				return;
			case opt:
				optimize();
				return;
			}

			throw new OBException("Incorrect operation mode");

		} catch (CmdLineException e) {
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
	 * 
	 * @param index
	 *            Index to load the objects into.
	 * @param load
	 *            File to load.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws OBStorageException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected abstract void addObjects(I index, File load)
			throws FileNotFoundException, IOException, OBStorageException,
			OBException, IllegalAccessException, InstantiationException;

	/**
	 * Opens a query file and queries the index storing all the results there.
	 * 
	 * @param index
	 *            The index to query.
	 * @param query
	 *            The query to load.
	 * @return ep Value if there was a validation. (0 if everything goes well)
	 *         or null otherwise
	 * @throws IOException
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected abstract void searchObjects(I index, File query, Statistics other)
			throws IOException, OBException, InstantiationException,
			IllegalAccessException;

	protected void create() throws IOException, OBStorageException,
			OBException, DatabaseException, InstantiationException,
			IllegalAccessException {
		// OBAsserts.chkFileNotExists(databaseFolder);
		OBAsserts.chkFileExists(load);

		A ambiente = instantiateNewAmbient(databaseFolder);
		I index = ambiente.getIndex();

		logger.info("Loading Data...");
		addObjects(index, load);
		/*
		 * logger.info("Closing..."); ambiente.close();
		 * logger.info("Re-opening..."); ambiente =
		 * instantiateNewAmbient(databaseFolder);
		 */
		logger.info("Freezing...");
		ambiente.freeze();

		logger.info(ambiente.getIndex().getStats());
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

	private void writeLine(FileWriter w, String[] data) throws IOException {

		String tab = "";
		for (String s : data) {
			w.write(tab);
			w.write(s);
			tab = "\t";
		}
		w.write("\n");
	}

	/**
	 * Process a list of experiments.
	 * 
	 * @return Returns the statistics for each set of experiments.
	 * @throws OBException
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private List<Pair<Statistics, Statistics>> processExperimentSet()
			throws OBException, IOException, DatabaseException,
			InstantiationException, IllegalAccessException {
		String[] sets = this.experimentSet.split(":");
		List<Pair<Statistics, Statistics>> result = new LinkedList<Pair<Statistics, Statistics>>();
		for (String set : sets) {
			logger.info("Executing experiment " + this.experimentName + " : "
					+ set);
			String[] rk = set.split(",");
			OBAsserts.chkAssert(rk.length == 2, "Wrong experiment set format");
			r = Double.parseDouble(rk[0]);
			k = Short.parseShort(rk[1]);
			result.add(searchAux());

		}
		return result;

	}

	/**
	 * Experiment set executes a number of experiments and then exists.
	 * 
	 * @throws OBException
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void experimentSet() throws OBException, IOException,
			DatabaseException, InstantiationException, IllegalAccessException {
		openIndex();
		processExperimentSet();
		closeIndex();
	}

	/**
	 * Execute processExperimentSet() several times and obtain a set of
	 * parameters that gives good results.
	 * 
	 * @throws OBException
	 * @throws IOException
	 * @throws DatabaseException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void optimize() throws OBException, IOException, DatabaseException,
			InstantiationException, IllegalAccessException {
		openIndex();
		EvolutionaryAlgorithmModule ea = new EvolutionaryAlgorithmModule(); 
	    ea.setGenerations(1000); 
	    ea.setAlpha(100);  
	    
	    OBOptimizerModule op = new OBOptimizerModule(getCreator(), this);
	    
		Collection<Module> modules = new ArrayList<Module>();
		modules.add(ea);
		modules.add(op);
		// setup opt4j
		Opt4JTask task = new Opt4JTask(true); 
	    task.init(modules); 
	    
	    try { 
	        task.execute(); 
	        Archive archive = task.getInstance(Archive.class); 
	   
	        for(Individual individual: archive){ 
	        	logger.info("Param: " + individual.getGenotype() );
	        	logger.info("Results: " + individual.getObjectives());	        	
	        } 
	   
	      } catch (Exception e) { 
	        throw new OBException(e);
	      } finally { 
	        task.close(); 
	      } 
	    
		closeIndex();
	}

	/**
	 * distance computations
	 */
	Objective distance = new Objective("distance", Sign.MIN);
	/**
	 * smap access count
	 */
	Objective smap = new Objective("smap", Sign.MIN);

	/**
	 * bucket access count
	 */
	Objective buckets = new Objective("buckets", Sign.MIN);

	/**
	 * Recall
	 */
	Objective recall = new Objective("recall", Sign.MAX);

	/**
	 * EP result
	 */
	Objective ep = new Objective("ep", Sign.MIN);
	
	@Override
	public Collection<Objective> getObjectives() {
		List<Objective> objs = new LinkedList();
		objs.add(distance);
		objs.add(smap);
		objs.add(buckets);
		objs.add(recall);
		objs.add(ep);
		return objs;
	}

	/**
	 * Returns the creator used to generate new configurations for this index.
	 * 
	 * @return a new creator
	 */
	protected abstract Class<? extends Creator<DoubleString>> getCreator();

	public Objectives evaluate(DoubleString config) {
		try {
			logger.info("Evaluating: " + config);
			updateIndexConfig(config);
			List<Pair<Statistics, Statistics>> stats = processExperimentSet();
			int totalQueries = 0;
			int failed = 0;
			int distances = 0;
			int smap = 0;
			int buckets = 0;
			double ep = 0;
			for(Pair<Statistics, Statistics> s : stats){
				totalQueries += s.getB().getQueryCount();
				failed += s.getB().getExtra("BAD");
				ep += s.getB().getStats("EP").mean();
				distances += s.getA().getDistanceCount();
				smap += s.getA().getSmapCount();
				buckets  += s.getA().getBucketsRead();
			}
			ep = ep / stats.size(); // normalize ep.
			double recall = 1d - (double)failed / (double)totalQueries ;
			 Objectives objectives = new Objectives();  
			 objectives.add(this.distance, distances);
			 objectives.add(this.smap, smap);
			 objectives.add(this.buckets, buckets);
			 objectives.add(this.recall, recall);
			 objectives.add(this.ep, ep);
			
			 return objectives;
			
		} catch (Exception e) {
			// the interface of the library
			// does not include exceptions, Hack!
			logger.fatal("Fatal error", e);
			System.exit(-1);
			return null;
		}
	}

	/**
	 * Updates the configuration of the index with the given phenotype.
	 * 
	 * @param phenotype
	 */
	protected abstract void updateIndexConfig(DoubleString phenotype);

	protected String expName() {
		return this.experimentName + "r" + this.r + "k";
	}

	private String p(double value) {
		return ((double) value / (double) queries) + "";
	}

	protected void writeAll(FileWriter[] files, String str) throws IOException {
		for (FileWriter f : files) {
			f.write(str);
			f.flush();
		}
	}

	protected void closeAll(FileWriter[] files) throws IOException {
		for (FileWriter f : files) {
			f.close();
		}
	}

	private void openIndex() throws IOException, OBStorageException,
			OBException, IllegalAccessException, InstantiationException {
		OBAsserts.chkFileExists(databaseFolder);
		OBAsserts.chkFileExists(query);
		ambiente = instantiateAmbient(databaseFolder);
		index = ambiente.getIndex();
	}

	private void closeIndex() throws OBException {
		ambiente.close();
	}

	protected Pair<Statistics, Statistics> searchAux() throws IOException,
			OBStorageException, OBException, DatabaseException,
			InstantiationException, IllegalAccessException {

		File d = new File(experimentResultFileName);

		FileWriter w = new FileWriter(d, true);
		if (d.length() == 0) {
			// write header if the file is empty
			writeLine(w, new String[] { "exp_name", "details", "dist", "smap",
					"ep", "bad", "zeros", "buckets" });
		}

		index.resetStats();
		logger.info("Searching... ");
		Statistics otherStats = new Statistics();
		logger.info("Searching with: " + expName());
		searchObjects(index, query, otherStats);
		logger.info(index.getStats().toString());
		logger.info(otherStats.toString());

		Statistics stats = index.getStats();
		writeLine(w, new String[] { expName(), expDetails(),
				String.valueOf(stats.getDistanceCount()),
				String.valueOf(stats.getSmapCount()),
				String.valueOf(otherStats.getStats("EP").mean()),
				String.valueOf(otherStats.getExtra("BAD")),
				String.valueOf(otherStats.getExtra("ZEROS")),
				String.valueOf(stats.getBucketsRead()), });

		w.close();
		return new Pair<Statistics, Statistics>(stats, otherStats);
	}

	protected abstract String expDetails();

	/**
	 * Perform one search for a given k and r.
	 * 
	 * @throws IOException
	 * @throws OBStorageException
	 * @throws OBException
	 * @throws DatabaseException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected void search() throws IOException, OBStorageException,
			OBException, DatabaseException, InstantiationException,
			IllegalAccessException {
		openIndex();
		searchAux();
		closeIndex();
	}

}
