package org.ajmm.obsearch.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractP2PIndex;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.IndexFactory;
import org.ajmm.obsearch.index.P2PIndexShort;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.SynchronizableIndexShort;
import org.ajmm.obsearch.index.pivotselection.TentaclePivotSelectorShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * This class shows how OBSearch can be used. The example is a P2P application.
 * The index consists of trees and some distance function. The initial data
 * input is provided by a plain text file with string representations of the
 * trees separated by newlines. A definition:
 * <p>
 * Spore: An xml file used to create an index. This file does not contain data.
 * It contains all the data necessary to store objects into an Index.
 * </p>
 * Before reviewing this example please review the example in
 * {@link #OBExampleTrees}. When the "-server" flag is given, the peer becomes
 * a rendezvous peer. Please check JXTA's documentation for more information on
 * rendezvous peers.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public final class OBSearchExample {

    /**
     * An utility class cannot have public constructors.
     */
    private OBSearchExample() {

    }

    /**
     * The logger used by the application.
     */
    private static Logger logger;

    /**
     * The seeds file contains some rendezvous peers (JXTA) that will be used as
     * initial seeders. Along with the spore, you have to give access to each
     * client to this file.
     */
    private static final File SEEDS = new File("seeds.txt");

    /**
     * The p2p index.
     */
    private static P2PIndexShort < OBSlice > p2pRef = null;

    /**
     * Main program function.
     * @param args
     *            Args sent from the command line.
     */
    public static void main(final String args[]) {
        int returnValue = 13;

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
            logger = Logger.getLogger(cline.getOptionValue("name"));

            File dbFolder = new File(cline.getOptionValue("db"));
            File stdFolder = new File(dbFolder, "std");
            File syncFolder = new File(dbFolder, "sync");
            File jxtaFolder = new File(dbFolder, "jxta");

            boolean server = cline.hasOption("server");
            // we crate one directory for each of the indexes
            // std contains the standard index
            if (!stdFolder.exists()) {
                if (!stdFolder.mkdirs()) {
                    throw new IOException("Error while creating dir: "
                            + stdFolder.toString());
                }
            }
            // a folder with the SynchronizableIndex
            if (!syncFolder.exists()) {
                if (!syncFolder.mkdirs()) {
                    throw new IOException("Error while creating dir: "
                            + syncFolder.toString());
                }
            }
            // a folder where jxta data can be stored
            if (!jxtaFolder.exists()) {
                if (!jxtaFolder.mkdirs()) {
                    throw new IOException("Error while creating dir: "
                            + jxtaFolder.toString());
                }
            }
            // number of threads to be used by
            // each peer
            int searchThreads = Integer.parseInt(cline
                    .getOptionValue("searchThreads"));
            // create a new p2p index
            if (cline.hasOption("create")) {
                logger.debug("Doing create");
                File data = new File(cline.getOptionValue("data"));
                // create the database
                byte od = Byte.parseByte(cline.getOptionValue("od"));
                short d = Byte.parseByte(cline.getOptionValue("d"));
                PPTreeShort < OBSlice > pp;
                // 0 and 1000 are the minimum and maximum values expected from
                // the
                // distance function. Note that you could do Short.MIN_VALUE and
                // Short.MAX_VALUE instead. But I believe that knowing the
                // ranges of
                // the distance function by means of
                // OBExampleTrees.shouldProcessSlice(s)
                // is a better and cleaner approach that helps to validate your
                // distance function.
                pp = new PPTreeShort < OBSlice >(stdFolder, d, od, (short) 0,
                        (short) 1000);
                SynchronizableIndexShort < OBSlice > index = new SynchronizableIndexShort < OBSlice >(
                        pp, syncFolder);

                // add data from a newline separated file with trees in string
                // form.
                logger.info("Adding data...");
                BufferedReader r = new BufferedReader(new FileReader(data));
                String re = r.readLine();
                int realIndex = 0;
                while (re != null) {
                    String l = OBExampleTrees.parseLine(re);
                    if (l != null) {
                        OBSlice s = new OBSlice(l);
                        if (OBExampleTrees.shouldProcessTree(s)) {
                            index.insert(s);
                            realIndex++;
                        }
                    }
                    re = r.readLine();
                }
                // generate pivots
                TentaclePivotSelectorShort < OBSlice > ps = new TentaclePivotSelectorShort < OBSlice >(
                        (short) 10, 5, new TreePivotable());
                ps.generatePivots((AbstractPivotIndex < OBSlice >) index
                        .getIndex());
                // freeze the index (or ask OBSearch to "learn" how the data is
                // distributed to optimize access
                // you can always add more data later.
                logger.info("freezing");
                index.freeze();
                index.close();

                // search the p2p index after we are in sync with everybody
            } else if (cline.hasOption("search")) {
                logger.debug("Doing search");
                P2PIndexShort < OBSlice > p2p = createP2PIndex(server, cline,
                        stdFolder, syncFolder, jxtaFolder, searchThreads);

                File data = new File(cline.getOptionValue("data"));
                // wait until our peer is connected properly, the peer must
                // sync all its data
                while (true) {
                    boolean peers = p2p.getNumberOfPeers() >= AbstractP2PIndex.minNumberOfPeers;
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

                // perform the match
                byte k = Byte.valueOf(cline.getOptionValue("k"));
                short range = Short.valueOf(cline.getOptionValue("r"));
                BufferedReader r = new BufferedReader(new FileReader(data));
                List < OBPriorityQueueShort < OBSlice >> result = new LinkedList < OBPriorityQueueShort < OBSlice >>();
                String re = r.readLine();
                int i = 0;
                long start = System.currentTimeMillis();
                while (re != null) {
                    String l = OBExampleTrees.parseLine(re);
                    if (l != null) {

                        if (i % 100 == 0) {
                            logger.info("Matching " + i);
                        }

                        OBSlice s = new OBSlice(l);
                        if (OBExampleTrees.shouldProcessTree(s)) {
                            OBPriorityQueueShort < OBSlice > x = new OBPriorityQueueShort < OBSlice >(
                                    k);
                            p2p.searchOB(s, range, x);
                            result.add(x);
                            i++;
                        }
                    }
                    // search until the 1642th tree
                    if (i == 1642) {
                        logger.warn("Finishing test at i : " + i);
                        break;
                    }
                    re = r.readLine();
                }
                while (p2p.isProcessingQueries()) {
                    try {
                        synchronized (r) {
                            logger.debug("Waiting for queries to complete...");
                            r.wait(1000);
                        }
                    } catch (InterruptedException e) {

                    }
                }
                long time = System.currentTimeMillis() - start;
                logger.info("Matched in: " + ((time / 1000) - 1) + " seconds");
               // p2p.close();
            } else if (cline.hasOption("tentacle")) {
                logger.debug("Doing tentacle");
                // just load the index and stay online indefinitely
                // helping others
                P2PIndexShort < OBSlice > p2p = createP2PIndex(server, cline,
                        stdFolder, syncFolder, jxtaFolder, searchThreads);
                while (true) {
                    try {
                        synchronized (logger) {
                            logger.wait(1000);
                        }
                    } catch (InterruptedException e) {

                    }
                }
            } else {
                throw new OBException(
                        "You have to set the mode: 'create' or 'search' or 'tentacle'");
            }
            returnValue = 0;
        } catch (ParseException exp) {
            logger.fatal("Argument parsing failed args: "
                    + Arrays.toString(args), exp);
            returnValue = 84;
            exp.printStackTrace();
        } catch (HelpException exp) {
            // no problem, we just display the help and quit
            logger.debug("Should have shown the help msg");
            exp.printStackTrace();
        } catch (Exception e) {
            logger.fatal("Exception caught", e);
            returnValue = 83;
            e.printStackTrace();
        }

        LogManager.shutdown();
        System.exit(returnValue);
    }

    /**
     * Creates a p2p index. This is one of the most important
     * @param server
     *            (Are we going to be a server or not)
     * @param cline
     *            (the command line object)
     * @param stdFolder
     *            (standard folder where the P+Tree is stored)
     * @param syncFolder
     *            (folder where the sync tree is stored)
     * @param jxtaFolder
     *            (this folder is used by jxta to store special information)
     * @param searchThreads
     *            (maximum number of threads to be used)
     * @return a P2P index
     * @throws Exception
     *             If something goes wrong :(
     */
    public static P2PIndexShort < OBSlice > createP2PIndex(
            final boolean server, final CommandLine cline,
            final File stdFolder, final File syncFolder, final File jxtaFolder,
            final int searchThreads) throws Exception {
        File spore = new File(cline.getOptionValue("spore"));
        if (!spore.exists()) {
            throw new OBException("Index file:" + spore + " does not exist.");
        }
        logger.info("Loading metadata and opening databases... file: "
                + spore.getAbsoluteFile());
        String sp = OBExampleTrees.readString(spore);
        // OBSearch works like a matryoshka doll, you wrap an index with another
        // to get extended functionality.
        // 1) The smallest index is the P+Tree
        PPTreeShort < OBSlice > pp = (PPTreeShort < OBSlice >) IndexFactory
                .createFromXML(sp);
        // required step to init restored databases
        pp.relocateInitialize(stdFolder);
        // 2) Then we wrap the previous index with a sync index
        // so that we can synchronize data among different indexes
        SynchronizableIndexShort < OBSlice > index = new SynchronizableIndexShort < OBSlice >(
                pp, syncFolder);
        // 3) Finally wrap the previous index with a p2p index so that we can
        // perform searches in a distributed way
        P2PIndexShort < OBSlice > p2p = new P2PIndexShort < OBSlice >(index,
                jxtaFolder, cline.getOptionValue("name"), searchThreads);
        logger.info("Opening Leecher");
        p2p.open(!server, true, SEEDS);
        p2pRef = p2p;
        return p2p;
    }

    /**
     * Initializes the command line definition. Here we define all the command
     * line options to be received by the program.
     * @return The options of the program.
     */
    public static Options initCommandLine() {
        final Option create = new Option(
                "create",
                "Create mode: Creates an Synchronizable Index and freezes it. After freezing it the program exists.  This index can be used later when executed in p2p mode");
        create.setRequired(false);

        final Option tentacle = new Option(
                "tentacle",
                "Tentacle mode: Connects to other peers and syncs with the network. Stays online undefinitely.");
        create.setRequired(false);

        final Option search = new Option(
                "search",
                "Search mode: By taking a spore, the index syncs with the network and then performs a search on the given data");
        create.setRequired(false);

        final Option server = new Option(
                "server",
                "Server mode: The peer becomes a rendezvous and relay peer. It will not search actively for new connections but it will be accessed by"
                        + " many peers");
        create.setRequired(false);

        final Option spore = OptionBuilder.withArgName("filename").hasArg()
                .withDescription("Spore to be fed into the system").create(
                        "spore");

        final Option in = OptionBuilder.withArgName("dir").hasArg().isRequired(
                true).withDescription("Database Directory").create("db");

        final Option out = OptionBuilder
                .withArgName("dir")
                .hasArg()
                .withDescription(
                        "Data File (new-line separated list of trees). Used for create or for search mode.")
                .create("data");

        final Option range = OptionBuilder.withArgName("<int>").hasArg()
                .withDescription("The range to be used in search mode").create(
                        "r");

        final Option name = OptionBuilder.withArgName("str").hasArg()
                .withDescription("The name of the peer").create("name");

        final Option k = OptionBuilder.withArgName("<byte>").hasArg()
                .withDescription("K to be used").create("k");

        final Option od = OptionBuilder.withArgName("#").hasArg()
                .withDescription("# of partitions for P+Tree").create("od");

        final Option d = OptionBuilder.withArgName("#").hasArg()
                .withDescription("# of dimensions.").create("d");

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
        options.addOption(d);
        options.addOption(searchThreads);
        options.addOption(tentacle);
        options.addOption(server);
        return options;
    }

}
