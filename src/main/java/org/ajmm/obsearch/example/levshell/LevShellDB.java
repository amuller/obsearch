package org.ajmm.obsearch.example.levshell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.example.OBString;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.DPrimeIndexShort;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.IncrementalBustosNavarroChavezShort;
import org.ajmm.obsearch.storage.bdb.BDBFactory;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.freehep.util.argv.ArgumentFormatException;
import org.freehep.util.argv.ArgumentParser;
import org.freehep.util.argv.BooleanOption;
import org.freehep.util.argv.IntOption;
import org.freehep.util.argv.ListParameter;
import org.freehep.util.argv.StringOption;
import org.freehep.util.argv.StringParameter;

import com.sleepycat.je.DatabaseException;
/**
 * This file starts java and  
 * 
 */
public class LevShellDB {

    private static final Logger logger = Logger.getLogger(LevShellDB.class);

    private static Properties props;

    // Setup all options and arguments
    static BooleanOption help = new BooleanOption("-help", "-h",
            "Show this help page", true);

    static BooleanOption version = new BooleanOption("-version", "-v",
            "Show product version", true);

    final static String dbFolder = System.getProperties().getProperty("user.home") + File.separator + ".levshell";
    
    static StringOption databaseFolder = new StringOption("-database", "-db", dbFolder,
            "Database Folder (default: " + dbFolder + "). All the entered commands will be stored here.");
    
    

    static BooleanOption create= new BooleanOption("-create", "-c",
            "Create a new DB from a list of files with commands. You can use the history files stored in your home. It is meant to be executed only once.");
    
    static BooleanOption standby= new BooleanOption("-standby", "-sb",
    "Start up and wait for commands from the client.");
    
    static IntOption pivots= new IntOption("-pivots", "-p", 18,
    "# of pivots to be employed.");
    
    static IntOption pivotsL= new IntOption("-lpivots", "-l", 1000,
    "l pairs of objects to select (bustos et al. pivot selection algorithm).");
    
    static IntOption pivotsM= new IntOption("-mpivots", "-m", 1000,
    "m pivot candidates will be selected (bustos et al. pivot selection algorithm).");

    static StringOption directory = new StringOption("-directory", "-d",
            "output dir", ".",
            "Output into directory instead of current directory");

    static StringOption property = new StringOption("-property", "-p",
            "property directory", "",
            "Read user property files from directory instead of current directory");

   

   

   
    static ListParameter files = new ListParameter("files", "Files to load into the DB");

    private static ArgumentParser initParams() {
        ArgumentParser cl = new ArgumentParser(
                "Example1 - example based on the AID compiler");
        cl.add(help);
        cl.add(version);
        cl.add(databaseFolder);
        cl.add(standby);
        cl.add(directory);
        cl.add(property);
        cl.add(pivots);
        cl.add(files);

        return cl;
    }

    public static void main(String args[]) throws Exception {
        try {
            
            try {
                initProperties();
            } catch (final Exception e) {
                System.err.print("Make sure log4j is configured properly"
                        + e.getMessage());
                e.printStackTrace();
                System.exit(48);
            }
            
            ArgumentParser cl = initParams();
            List extra = cl.parse(args);

            if (!extra.isEmpty() || help.getValue()) {
                cl.printUsage(System.out);
                return;
            }

            if (version.getValue()) {
                System.out.println(props.getProperty("version"));
                return;
            }
            
            File db = new File(databaseFolder.getValue() + File.separator +  "storage");
                      
            if(create.getValue()){
                if(!db.exists()){
                    db.mkdirs();
                }else{
                    throw new IOException("Folder " + db + " already exists, cannot create DB here");
                }
                BDBFactory fact = new BDBFactory(db);
                IncrementalBustosNavarroChavezShort<OBString> ps = new
                IncrementalBustosNavarroChavezShort<OBString>(new AcceptAll(),
                         pivotsL.getInt(), pivotsM.getInt());
                if(! (pivots.getInt() <= Byte.MAX_VALUE)){
                    throw new IllegalArgumentException("Cannot use more than " + Byte.MAX_VALUE + " pivots");
                }
                byte piv = (byte)pivots.getInt();
                //IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector();               
                DPrimeIndexShort < OBString > index = new DPrimeIndexShort < OBString >(
                        fact, piv, ps, OBString.class);
                
                Iterator<String> fileNamesIt = files.getValue().iterator(); 
                while(fileNamesIt.hasNext()){
                    String fileName = fileNamesIt.next();
                    insertFile(fileName, index);
                }
                
                index.freeze();
                index.close();
                FileWriter out = new FileWriter( new File(databaseFolder.getValue(), Index.SPORE_FILENAME));
                out.write(index.toXML());
                
            }else if(standby.getValue()){
                
                // create an RCP
                
            }else{
                throw new ArgumentFormatException("Must use mode: " + create.getOption() + " or " + standby.getOption() );
            }

            

        } catch (ArgumentFormatException afe) {
            System.out.println(afe.getMessage());
            return;
        }

    }
    
    private static void insertFile(String file, IndexShort < OBString > index) throws FileNotFoundException, IOException, DatabaseException, InstantiationException, IllegalAccessException, OBException{
       
        BufferedReader r = new BufferedReader(new FileReader(file));
        String re = r.readLine();
        while (re != null) {
            index.insert(new OBString(re));
            re = r.readLine();
        }
    }
    
    

    public static void initProperties() throws IOException {

        InputStream is = LevShellDB.class.getResourceAsStream(property.getOption() + File.separator
                + "general.properties");
        props = new Properties();
        props.load(is);
        // configure log4j only once too
        PropertyConfigurator.configure(props.getProperty("log4j.file"));
    }

}
