package org.ajmm.obsearch.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.index.IndexFactory;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.ParallelIndexShort;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
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

import com.thoughtworks.xstream.XStream;

public class OBExampleTrees {
	private static final Logger logger = Logger.getLogger(OBExampleTrees.class);
	
	public static void main(String[] args){
		int returnValue = 0;
		try{
			PropertyConfigurator.configure("obexample.log4j");
		}catch(Exception e){
				System.err.print("Make sure log4j is configured properly" + e.getMessage()); // NOPMD by amuller on 11/18/06 7:32 PM
				e.printStackTrace();
				System.exit(48);
		}
		try{
		
			CommandLine cline = getCommandLine(initCommandLine(), OBExampleTrees.class, args);
			PPTreeShort<OBSlice> index;
			File data = new File(cline.getOptionValue("data"));
			if(cline.hasOption("create")){
				
				//create the database
				
		        File dbFolder = new File(cline.getOptionValue("db"));
				byte od = Byte.parseByte(cline.getOptionValue("od"));
		        index = new PPTreeShort<OBSlice>(
		                dbFolder, (short) 30, (byte) od, (short)0, (short) 1000);
		    	
		    	
		        logger.info("Adding data");
	            BufferedReader r = new BufferedReader(new FileReader(data));
	            String re = r.readLine();
	            int realIndex = 0;
	            while (re != null) {
	                String l = parseLine(re);
	                if (l != null) {
	                	OBSlice s = new OBSlice(l);
	                	if(shouldProcessSlice(s)){
	                		index.insert(s, realIndex);
	                		realIndex++;
	                	}
	                }
	                re = r.readLine();
	            }
	            // generate pivots
	            //DummyPivotSelector ps = new DummyPivotSelector();
	            TentaclePivotSelectorShort ps = new TentaclePivotSelectorShort((short)10);
	            ps.generatePivots((AbstractPivotIndex)index);
	            // the pyramid values are created
	            logger.info("freezing");
	            index.freeze();
		        index.close();
		        logger.info("Finished Index Creation");
			}else if(cline.hasOption("search")){
				
				
				// LOAD DB
				
				File dbFolder = new File(cline.getOptionValue("db"));

		        // TODO: clean the way we obtain the index file name
		        // maybe just using one name is fine
		        File indexFile = new File(dbFolder + "/PPTreeShort");
		        if(! indexFile.exists()){
		        	throw new OBException("Index file:" + indexFile + " does not exist.");
		        }
		        logger.info("Loading metadata and opening databases... file: " + indexFile.getAbsoluteFile());
		       
		        
		        index = (PPTreeShort<OBSlice>)IndexFactory.createFromXML(readString(indexFile));
		        // required step to init databases
		        index.relocateInitialize(null);
		        
				logger.info("Done! DB size: " + index.databaseSize());
				 byte k = Byte.parseByte(cline.getOptionValue("k"));
		         short range = Short.parseShort(cline.getOptionValue("r")); // range
		         BufferedReader r = new BufferedReader(new FileReader(data));
	             List<OBPriorityQueueShort<OBSlice>> result =
					new LinkedList<OBPriorityQueueShort<OBSlice>>();
	             String re = r.readLine();
	            int i = 0;
	            long start = System.currentTimeMillis();
				while (re != null) {
	                String l = parseLine(re);
	                if (l != null) {
	                	OBPriorityQueueShort<OBSlice> x = new OBPriorityQueueShort<OBSlice>(k);
	                    if (i % 100 == 0) {
	                        logger.info("Matching " + i);
	                    }

	                    OBSlice s = new OBSlice(l);
	                	if(shouldProcessSlice(s)){
	                		index.searchOB(s, range, x);
	                    	result.add(x);
	                    	i++;
	                	}
	                }
	                if(i == 1642){
	                    logger.warn("Finishing test at i : " + i);
	                    break;
	                }
	                re = r.readLine();
	            }
				//pindex.waitQueries();
				long time = System.currentTimeMillis() - start;
				logger.info("Running time: seconds: " + time / 1000 +  " minutes: " + time / 1000 / 60);
				logger.info(index);
				
			}else{
				throw new OBException("You have to set the mode: 'create' or 'search'");
			}
			
		}catch( ParseException exp ) {
	        logger.fatal( "Argument parsing failed args: " + args, exp );
	        returnValue = 84;
	    }
	    catch(HelpException exp){
	    	// no problem, we just display the help and quit
	    }
	    catch(Exception e){
	    	logger.fatal( "Exception caught", e);
	    	returnValue =83;
	    }
	    LogManager.shutdown();
	    System.exit(returnValue);
	}
	
	public static String readString(File file) throws IOException{
		 StringBuilder res = new StringBuilder();
		 BufferedReader metadata = new BufferedReader(new FileReader(file));
		 String r = metadata.readLine();
		 while(r != null){
			 res.append(r);
			 r = metadata.readLine();
		 }
		 return res.toString();
	}
	
	
	public static CommandLine getCommandLine(final Options options, final Class c, final String[] args) throws ParseException, HelpException {
		final CommandLineParser parser = new GnuParser();
		Option help = new Option( "help", "print this message" );
        options.addOption(help);
        final CommandLine line = parser.parse(options , args );
        // add the "help option to the help" :)
        if(line.hasOption("help")){
        	final HelpFormatter formatter = new HelpFormatter();
        	formatter.printHelp(c.getName(), options, true );
        	throw new HelpException();
        }
        return line;
	}
	public static Options initCommandLine(){
		final Option create = new Option("create", "Create mode");
		create.setRequired(false);
		
		final Option search = new Option("search", "Search mode");
		create.setRequired(false);
		
		
		final Option in   = OptionBuilder.withArgName( "dir" )
        .hasArg()
        .isRequired(true)
        .withDescription(  "Database Directory" )
        .create( "db" );
        
		final Option out  = OptionBuilder.withArgName( "dir" )
        .hasArg()
        .isRequired(true)
        .withDescription(  "Data File (new-line separated list of trees). Used for create or for search mode." )
        .create( "data" );
		
		final Option range  = OptionBuilder.withArgName( "dir" )
        .hasArg()
        .withDescription(  "The range to be used" )
        .create( "r" );
		
		final Option k = OptionBuilder.withArgName( "dir" )
        .hasArg()
        .withDescription( "K to be used" )
        .create( "k" );
		
		final Option od = OptionBuilder.withArgName( "#" )
        .hasArg()
        .withDescription( "# of partitions for P+Tree" )
        .create( "od" );
		
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
	
	public static boolean shouldProcessSlice(OBSlice x) throws Exception{
    	return x.size()<= 500;
    }

    public static String parseLine(String line) {
        if (line.startsWith("//") || "".equals(line.trim())
                || (line.startsWith("#") && !line.startsWith("#("))) {
            return null;
        } else {
            String arr[] = line.split("[:]");
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
