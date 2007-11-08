package org.ajmm.obsearch.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.IndexFactory;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.KMeansPPPivotSelector;
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
 * This class shows how a tree matcher can be built with OBSearch. Please see
 * the ant file example.xml (targets "create" and "search") for an example on
 * how to invocate this program.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public final class OBExampleTrees {

    /**
     * Logger.
     */
    private static final Logger logger = Logger.getLogger("OBExampleTrees");

    /**
     * Utility classes should not have public constructors.
     */
    private OBExampleTrees() {

    }

    /**
     * Reads the parameters from the command line and either creates or searches
     * an index. 
     * @param args
     *            The arguments from the command line.
     */
    public static void main(final String[] args) {
        int returnValue = 0;
        int maxStrSize = 0;
        // initialize log4j
        try {
            PropertyConfigurator.configure("obexample.log4j");
        } catch (final Exception e) {
            System.err.print("Make sure log4j is configured properly"
                    + e.getMessage());
            e.printStackTrace();
            System.exit(48);
        }
        try {

            final CommandLine cline = getCommandLine(initCommandLine(),
                    OBExampleTrees.class, args);
            PPTreeShort < OBSlice > index;
            final File data = new File(cline.getOptionValue("data"));
            if (cline.hasOption("create")) {

                final File dbFolder = new File(cline.getOptionValue("db"));
                final byte od = Byte.parseByte(cline.getOptionValue("od"));
                // create the index
                /*
                 * 30 is the number of pivots to use. 0 and 1000 are the minimum
                 * and maximum expected values that the function distance in
                 * OBSlice will return. (based on the maximum tree value defined
                 * by shouldProcessTree())
                 */
                index = new PPTreeShort < OBSlice >(dbFolder, (short) 30, od,
                        (short) 0, (short) 7000);

                logger.info("Adding data");
                final BufferedReader r = new BufferedReader(
                        new FileReader(data));
                String re = r.readLine();
                int realIndex = 0;
                // read the trees file
                while (re != null) {
                    // if this returns null then we read a comment
                    final String l = parseLine(re);
                    int tl = l.length();
                    if(maxStrSize < tl){
                        maxStrSize = tl;
                    }
                    if (l != null) {
                        final OBSlice s = new OBSlice(l);
                        if (shouldProcessTree(s)) {
                            // insert the tree in the database
                            index.insert(s);
                            realIndex++;
                        }
                    }
                    re = r.readLine();
                }
                // 19413
                logger.info("Max str size: " + maxStrSize);
                // generate pivots
                // The object ps will select pivots based on some simple
                // criteria
                final TentaclePivotSelectorShort < OBSlice > ps = new TentaclePivotSelectorShort < OBSlice >((short) 10, 30, new AcceptAll< OBSlice >());
                logger.debug("Selecting pivots");
                //KMeansPPPivotSelector<OBSlice> ps = new KMeansPPPivotSelector<OBSlice>(new TreePivotable< OBSlice >());
                ps.generatePivots(index);
                // Freeze the index so that we can start using it
                logger.info("Freezing");
                index.freeze();
                // close the index
                logger.info("Finished Index Creation, items stored:" + index.databaseSize());                
                index.close();                
            } else if (cline.hasOption("search")) {

                // Load a database created in the previous step and search
                // it.
                final File dbFolder = new File(cline.getOptionValue("db"));

                // The spore is an xml file that contains information created
                // after the "learning" process done during Freeze().
                // In the case of the PPTreeShort index, it will always be
                // stored
                // in the folder where the DB was created under the file:
                // PPTreeShort
                final File indexFile = new File(dbFolder + "/PPTreeShort");
                if (!indexFile.exists()) {
                    throw new OBException("Index file:" + indexFile
                            + " does not exist.");
                }
                logger.info("Loading metadata and opening databases... file: "
                        + indexFile.getAbsoluteFile());
                // We simply load the spore by using IndexFactory...
                index = (PPTreeShort < OBSlice >) IndexFactory
                        .createFromXML(readString(indexFile));
                // and initialize it.
                index.relocateInitialize(null);

                logger.info("Done! DB size: " + index.databaseSize());
                final byte k = Byte.parseByte(cline.getOptionValue("k"));
                final short range = Short.parseShort(cline.getOptionValue("r"));
                final BufferedReader r = new BufferedReader(
                        new FileReader(data));
                // we keep the results of the match in a list.
                final List < OBPriorityQueueShort < OBSlice >> result = new LinkedList < OBPriorityQueueShort < OBSlice >>();
                String re = r.readLine();
                int i = 0;
                final long start = System.currentTimeMillis();
                // we will read one by one trees separated by new-line
                // from the given file, and perform a search using the
                // given k and range
                while (re != null) {
                    final String l = parseLine(re);
                    if (l != null) {
                        // this is where the result of the match will be stored
                        final OBPriorityQueueShort < OBSlice > x = new OBPriorityQueueShort < OBSlice >(
                                k);
                        if (i % 100 == 0) {
                            logger.info("Matching " + i);
                        }

                        final OBSlice s = new OBSlice(l);
                        // we load the Tree from the file
                        if (shouldProcessTree(s)) {
                            // and perform the search
                            //logger.info("Slice: " + l);
                            index.searchOB(s, range, x);
                            // search is completed, we just store the
                            // result.
                            //logger.info(x);
                            //result.add(x);
                            i++;
                        }
                    }
                    // hardcoded value, we don't match beyond this
                    // value
                    if (i == 1642) {
                        logger.warn("Finishing test at i : " + i);
                        break;
                    }
                    re = r.readLine();
                }
                // we can do something now with the result
                final long time = System.currentTimeMillis() - start;
                logger.info("Running time in seconds: " + time / 1000
                        + " minutes: " + time / 1000 / 60);
                logger.info("Stats follow: total initial rectangles:" + index.initialHyperRectangleTotal + " final initial rectangles " + index.finalHyperRectangleTotal);
                logger.info("Status detail: pyramids accessed: " + index.finalPyramidTotal + " smap vectors: " + index.smapRecordsCompared + " distance computations: " + index.distanceComputations);
            } else {
                throw new OBException(
                        "You have to set the mode: 'create' or 'search'");
            }

        } catch (final ParseException exp) {
            logger.fatal("Argument parsing failed args: "
                    + Arrays.toString(args), exp);
            returnValue = 84;
        } catch (final HelpException exp) {
            // no problem, we just display the help and quit
            logger.debug("Should have shown the help msg");
        } catch (final Exception e) {
            logger.fatal("Exception caught", e);
            returnValue = 83;
        } finally {
            LogManager.shutdown();
            System.exit(returnValue);
        }
    }

    /**
     * Reads a String from the given file.
     * @param file
     *            File to Read
     * @return A String representation of the file
     * @throws IOException
     *             If there is an IO error
     */
    public static String readString(final File file) throws IOException {
        final StringBuilder res = new StringBuilder();
        final BufferedReader metadata = new BufferedReader(new FileReader(file));
        String r = metadata.readLine();
        while (r != null) {
            res.append(r);
            r = metadata.readLine();
        }
        metadata.close();
        return res.toString();
    }

    /**
     * Parses the array of options as received in main() and returns a
     * CommandLine object that makes it easier to analyze the commands.
     * @param options
     *            The options object (generated from initCommandLine(...))
     * @param c
     *            The class that will be used for the name of the program.
     * @param args
     *            Arguments of the command line (as received in main(...))
     * @return A CommandLine object ready to parse commands
     * @throws ParseException
     *             If the given arguments have syntax errors
     * @throws HelpException
     *             If the user wants "help" we generate an exception
     */
    public static CommandLine getCommandLine(final Options options,
            final Class c, final String[] args) throws ParseException,
            HelpException {
        final CommandLineParser parser = new GnuParser();
        final Option help = new Option("help", "print this message");
        options.addOption(help);
        final CommandLine line = parser.parse(options, args);
        // add the "help option to the help" :)
        if (line.hasOption("help")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(c.getName(), options, true);
            throw new HelpException();
        }
        return line;
    }

    /**
     * Initializes the command line definition. Here we define all the command
     * line options to be received by the program.
     * @return The options of the program.
     */
    public static Options initCommandLine() {
        final Option create = new Option("create", "Create mode");
        create.setRequired(false);

        final Option search = new Option("search", "Search mode");
        create.setRequired(false);

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

        final Option k = OptionBuilder.withArgName("dir").hasArg()
                .withDescription("K to be used").create("k");

        final Option od = OptionBuilder.withArgName("#").hasArg()
                .withDescription("# of partitions for P+Tree").create("od");

        Options options = new Options();
        options.addOption(in);
        options.addOption(out);
        options.addOption(range);
        options.addOption(create);
        options.addOption(k);
        options.addOption(search);
        options.addOption(od);
        return options;
    }

    /**
     * We ignore trees whose size (in nodes) is greater than the value hardcoded
     * below.
     * @param x
     *            The object to be matched.
     * @return True if we should process the given tree.
     * @throws Exception
     *             Throws an exception if the OBSlice has an invalid tree
     */
    public static boolean shouldProcessTree(final OBSlice x) throws Exception {
        return true;
    }

    /**
     * Parses the given line from the files that contain string representations
     * of trees.
     * @param line
     *            A line in the trees file
     * @return A string representation of a Tree or null if the line was a
     *         comment.
     */
    public static String parseLine(final String line) {
        if (line.startsWith("//") || "".equals(line.trim())
                || (line.startsWith("#") && !line.startsWith("#("))) {
            return null;
        } else {
            final String[] arr = line.split("[:]");
            if (arr.length == 2) {
                return arr[1];
            } else if (arr.length == 1) {
                return arr[0];
            } else {
                assert false : "Received line: " + line;
                return null;
            }
        }
    }

}
