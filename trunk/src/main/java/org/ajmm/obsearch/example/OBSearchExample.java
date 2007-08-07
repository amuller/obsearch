package org.ajmm.obsearch.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.IndexFactory;
import org.ajmm.obsearch.index.P2PIndexShort;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.SynchronizableIndexShort;
import org.ajmm.obsearch.index.pivotselection.TentaclePivotSelectorShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * This class shows how OBSearch can be used. The example is a P2P application.
 * The index consists of trees and some distance function. The initial data
 * input is provided by a plain text file with string representations of the
 * trees separated by newlines. Some definitions: Pollination: Transfers data
 * from one node A to a node B. (data sync). Spore: An xml file used to create
 * an index. This file does not contain data. It contains all the data necessary
 * to store objects into an Index.
 */
public class OBSearchExample {

    private static final Logger logger = Logger
	    .getLogger(OBSearchExample.class);

    private static final File seeds = new File("seeds.txt");

    public static void main(String args[]) {
	int returnValue = 0;
	// initialize log4j
	try {
	    PropertyConfigurator.configure("obexample.log4j");
	} catch (Exception e) {
	    System.err.print("Make sure log4j is configured properly"
		    + e.getMessage()); // NOPMD by amuller on 11/18/06 7:32 PM
	    e.printStackTrace();
	    System.exit(48);
	}
	try {
	    // main code of the application goes here
	    CommandLine cline = OBExampleTrees.getCommandLine(
		    initCommandLine(), OBSearchExample.class, args);

	    File data = new File(cline.getOptionValue("data"));
	    File dbFolder = new File(cline.getOptionValue("db"));
	    File stdFolder = new File(dbFolder, "std");
	    File syncFolder = new File(dbFolder, "sync");
	    File jxtaFolder = new File(dbFolder, "jxta");

	    if (!stdFolder.mkdirs()) {
		throw new IOException();
	    }

	    if (!syncFolder.mkdirs()) {
		throw new IOException();
	    }

	    if (!jxtaFolder.mkdirs()) {
		throw new IOException();
	    }

	    int searchThreads = Integer.parseInt(cline
		    .getOptionValue("searchThreads"));

	    if (cline.hasOption("create")) {

		// create the database
		byte od = Byte.parseByte(cline.getOptionValue("od"));
		PPTreeShort<OBSlice> pp;
		pp = new PPTreeShort<OBSlice>(stdFolder, (short) 30, (byte) od,
			(short) 0, (short) 1000);
		SynchronizableIndexShort<OBSlice> index = new SynchronizableIndexShort<OBSlice>(
			pp, syncFolder);

		logger.info("Adding data");
		BufferedReader r = new BufferedReader(new FileReader(data));
		String re = r.readLine();
		int realIndex = 0;
		while (re != null) {
		    String l = OBExampleTrees.parseLine(re);
		    if (l != null) {
			OBSlice s = new OBSlice(l);
			if (OBExampleTrees.shouldProcessSlice(s)) {
			    index.insert(s);
			    realIndex++;
			}
		    }
		    re = r.readLine();
		}
		// generate pivots
		// DummyPivotSelector ps = new DummyPivotSelector();
		TentaclePivotSelectorShort<OBSlice> ps = new TentaclePivotSelectorShort<OBSlice>(
			(short) 10);
		ps.generatePivots((AbstractPivotIndex<OBSlice>) index
			.getIndex());
		// the pyramid values are created
		logger.info("freezing");
		index.freeze();

		P2PIndexShort<OBSlice> p2p = new P2PIndexShort<OBSlice>(index,
			jxtaFolder, cline.getOptionValue("name"), searchThreads);
		logger.info("Opening Seeder");
		p2p.open(false, true, seeds);
		// index.close();

		// wait until we have a minimum number of peers connected, and
                // their
		// boxes are up to date with ours

		while (true) {
		    boolean peers = p2p.getNumberOfPeers() >= 2;
		    boolean sync = p2p.areAllPeersSynchronizedWithMe();
		    boolean boxes = p2p.areAllBoxesAvailable();
		    
		    if (peers && sync && boxes) {
			break;
			
		    }
		    
		    synchronized (dbFolder) {
			dbFolder.wait(10000);
		    }
		    
		    logger.debug("Waiting for conditions! peers: " + peers
			    + " sync: " + sync + " boxes: " + boxes);
		}
		logger.debug("Going to perform the match, we are ready! ");

		byte k = 3;
		short range = 3;
		r = new BufferedReader(new FileReader(data));
		List<OBPriorityQueueShort<OBSlice>> result = new LinkedList<OBPriorityQueueShort<OBSlice>>();
		re = r.readLine();
		int i = 0;
		long start = System.currentTimeMillis();
		while (re != null) {
		    String l = OBExampleTrees.parseLine(re);
		    if (l != null) {
			OBPriorityQueueShort<OBSlice> x = new OBPriorityQueueShort<OBSlice>(
				k);
			if (i % 100 == 0) {
			    logger.info("Matching " + i);
			}

			OBSlice s = new OBSlice(l);
			if (OBExampleTrees.shouldProcessSlice(s)) {
			    if(logger.isDebugEnabled()){
				    logger.debug("Starting query # " + i);
			    }
			    p2p.searchOB(s, range, x);
			    result.add(x);
			    i++;
			}
		    }
		    if (i == 1642) {
			logger.warn("Finishing test at i : " + i);
			break;
		    }
		    re = r.readLine();
		    // break; // just do it once
		}
		while (p2p.isProcessingQueries()) {
		    try {
			synchronized (r) {
			    logger.debug("Waiting for queries to complete...");
			    r.wait(5000);
			}
		    } catch (InterruptedException e) {

		    }
		}
		long time = System.currentTimeMillis() - start;
		logger.info("Matched in: " + (time / 1000) + " seconds");
		// show the result:
		if (logger.isDebugEnabled()) {
		    logger.debug("Results!");
		    Iterator<OBPriorityQueueShort<OBSlice>> it = result
			    .iterator();
		    while (it.hasNext()) {
			OBPriorityQueueShort<OBSlice> t = it.next();
			logger.debug(t);
		    }
		}
		// pindex.waitQueries();

	    } else if (cline.hasOption("search")) {

		// TODO: clean the way we obtain the index file name
		// maybe just using one name is fine
		File spore = new File(cline.getOptionValue("spore"));
		if (!spore.exists()) {
		    throw new OBException("Index file:" + spore
			    + " does not exist.");
		}
		logger.info("Loading metadata and opening databases... file: "
			+ spore.getAbsoluteFile());

		PPTreeShort<OBSlice> pp = (PPTreeShort<OBSlice>) IndexFactory
			.createFromXML(OBExampleTrees.readString(spore));
		// required step to init databases
		pp.relocateInitialize(stdFolder);
		SynchronizableIndexShort<OBSlice> index = new SynchronizableIndexShort<OBSlice>(
			pp, syncFolder);
		P2PIndexShort<OBSlice> p2p = new P2PIndexShort<OBSlice>(index,
			jxtaFolder, cline.getOptionValue("name"), searchThreads);
		logger.info("Opening Leecher");
		p2p.open(true, true, seeds);

		while (true) { // wait undefinitely
		    synchronized (dbFolder) {
			dbFolder.wait(10000);
		    }
		}
		/*
                 * logger.info("Done! DB size: " + pp.databaseSize()); byte k =
                 * Byte.parseByte(cline.getOptionValue("k")); short range =
                 * Short.parseShort(cline.getOptionValue("r")); // range
                 * BufferedReader r = new BufferedReader(new FileReader(data));
                 * List<OBPriorityQueueShort<OBSlice>> result = new LinkedList<OBPriorityQueueShort<OBSlice>>();
                 * String re = r.readLine(); int i = 0; long start =
                 * System.currentTimeMillis(); while (re != null) { String l =
                 * OBExampleTrees.parseLine(re); if (l != null) {
                 * OBPriorityQueueShort<OBSlice> x = new OBPriorityQueueShort<OBSlice>(k);
                 * if (i % 100 == 0) { logger.info("Matching " + i); }
                 * 
                 * OBSlice s = new OBSlice(l);
                 * if(OBExampleTrees.shouldProcessSlice(s)){ index.searchOB(s,
                 * range, x); result.add(x); i++; } } if(i == 1642){
                 * logger.warn("Finishing test at i : " + i); break; } re =
                 * r.readLine(); } //pindex.waitQueries(); long time =
                 * System.currentTimeMillis() - start; logger.info("Running
                 * time: seconds: " + time / 1000 + " minutes: " + time / 1000 /
                 * 60); logger.info(index);
                 */

	    } else {
		throw new OBException(
			"You have to set the mode: 'create' or 'search'");
	    }

	} catch (ParseException exp) {
	    logger.fatal("Argument parsing failed args: " + args, exp);
	    returnValue = 84;
	} catch (HelpException exp) {
	    // no problem, we just display the help and quit
	} catch (Exception e) {
	    logger.fatal("Exception caught", e);
	    returnValue = 83;
	} finally {
	    LogManager.shutdown();
	    System.exit(returnValue);
	}
    }

    public static Options initCommandLine() {
	final Option create = new Option("create",
		"Create mode: Creates an Index and leaves the connections open for Pollination");
	create.setRequired(false);

	final Option search = new Option(
		"search",
		"Search mode: By taking a spore, the index syncs with the network and then performs a search on the given data");
	create.setRequired(false);

	final Option spore = OptionBuilder.withArgName("filename").hasArg()
		.withDescription("Spore to be fed into the system").create(
			"spore");

	final Option in = OptionBuilder.withArgName("dir").hasArg().isRequired(
		true).withDescription("Database Directory").create("db");

	final Option out = OptionBuilder
		.withArgName("dir")
		.hasArg()
		.isRequired(true)
		.withDescription(
			"Data File (new-line separated list of trees). Used for create or for search mode.")
		.create("data");

	final Option range = OptionBuilder.withArgName("dir").hasArg()
		.withDescription("The range to be used").create("r");

	final Option name = OptionBuilder.withArgName("str").hasArg()
		.withDescription("The of the peer").create("name");

	final Option k = OptionBuilder.withArgName("dir").hasArg()
		.withDescription("K to be used").create("k");

	final Option od = OptionBuilder.withArgName("#").hasArg()
		.withDescription("# of partitions for P+Tree").create("od");

	final Option searchThreads = OptionBuilder.withArgName("#").hasArg()
		.isRequired(true).withDescription("# of search threads")
		.create("searchThreads");

	Options options = new Options();
	options.addOption(in);
	options.addOption(out);
	options.addOption(range);
	options.addOption(create);
	options.addOption(k);
	options.addOption(search);
	options.addOption(od);
	options.addOption(spore);
	options.addOption(name);
	options.addOption(searchThreads);
	return options;
    }

}
