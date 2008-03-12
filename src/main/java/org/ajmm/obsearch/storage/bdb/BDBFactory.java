package org.ajmm.obsearch.storage.bdb;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

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

import java.io.File;
import java.io.IOException;

import org.ajmm.obsearch.asserts.OBAsserts;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.storage.OBStore;
import org.ajmm.obsearch.storage.OBStoreByte;
import org.ajmm.obsearch.storage.OBStoreDouble;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreFloat;
import org.ajmm.obsearch.storage.OBStoreInt;
import org.ajmm.obsearch.storage.OBStoreLong;
import org.ajmm.obsearch.storage.OBStoreShort;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * BDBFactory generates an environment in the given directory, and creates
 * databases as OBSearch requests.
 * @author Arnoldo Jose Muller Molina
 */

public class BDBFactory implements OBStoreFactory {

    /**
     * The environment.
     */
    private Environment env;

    /**
     * Creates a new factory that will be based in the given directory.
     * @param directory
     *                The directory where the Berkeley DB files will be stored.
     * @throws IOException
     *                 If the given directory does not exist.
     */
    public BDBFactory(File directory) throws IOException, DatabaseException {
        directory.mkdirs();
        OBAsserts.chkFileExists(directory);
        EnvironmentConfig envConfig = createEnvConfig();
        env = new Environment(directory, envConfig);
    }
    
    /**
     * Creates the default environment configuration.
     * @return Default environment configuration.
     */
    private EnvironmentConfig createEnvConfig() {
        /* Open a transactional Oracle Berkeley DB Environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        // the default value worked pretty well.
        // envConfig.setCacheSize(CACHE_SIZE);
        // leaving this, but it is meaningless when setLocking(false)
        //envConfig.setTxnNoSync(true);
        envConfig.setConfigParam("java.util.logging.DbLogHandler.on", "false");
        // 100 k gave the best performance in one thread and for 30 pivots of
        // shorts
        //envConfig.setConfigParam("je.log.faultReadSize", "30720");
        // envConfig.setConfigParam("je.log.faultReadSize", "10240");
        // alternate access method might be the best. We got to keep all the
        // btree in memory
        // envConfig.setConfigParam("je.evictor.lruOnly", "false");
        // envConfig.setConfigParam("je.evictor.nodesPerScan", "100");
        // envConfig.setTxnNoSync(true);
        // envConfig.setTxnWriteNoSync(true);
        // disable this in production
        // envConfig.setLocking(false);

        return envConfig;
    }

    public void close() throws OBStorageException {
        try {
            env.cleanLog();
            env.compress();
            env.checkpoint(null);
            env.close();
        } catch (DatabaseException e) {
            throw new OBStorageException(e);
        }
    }

    public OBStore createOBStore(String name, boolean temp) throws OBStorageException{       
        OBStore res = null;
        try{
            res = new BDBOBStore(name, env.openDatabase(null, name, null));
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
       return res;
    }


    public OBStoreByte createOBStoreByte(String name, boolean temp) {
        // TODO Auto-generated method stub
        return null;
    }


    public OBStoreDouble createOBStoreDouble(String name, boolean temp) {
        // TODO Auto-generated method stub
        return null;
    }


    public OBStoreFloat createOBStoreFloat(String name, boolean temp) {
        // TODO Auto-generated method stub
        return null;
    }


    public OBStoreInt createOBStoreInt(String name, boolean temp) {
        // TODO Auto-generated method stub
        return null;
    }


    public OBStoreLong createOBStoreLong(String name, boolean temp) {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * Creates a default database configuration.
     * @return default database configuration.
     */
    protected DatabaseConfig createDefaultDatabaseConfig() {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(false);
        return dbConfig;
    }

    
    public OBStoreShort createOBStoreShort(String name, boolean temp) throws OBStorageException{
        
        OBStoreShort res = null;
        try{
            DatabaseConfig dbConfig = createDefaultDatabaseConfig();
            res = new BDBOBStoreShort(name, env.openDatabase(null, name, dbConfig));
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
       return res;
    }
    

}
