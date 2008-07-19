package net.obsearch.storage.bdb;
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

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBStorageException;

import net.obsearch.storage.OBStore;

<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
import net.obsearch.storage.OBStore${Type};
</#list>

import net.obsearch.storage.TupleBytes;

import net.obsearch.storage.OBStoreFactory;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;

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
        envConfig.setConfigParam("java.util.logging.DbLogHandler.on", "false");
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

    public OBStore<TupleBytes> createOBStore(String name, boolean temp, boolean duplicates) throws OBStorageException{       
        OBStore res = null;
        DatabaseConfig dbConfig = createDefaultDatabaseConfig();
					if(duplicates){
								dbConfig.setSortedDuplicates(true);
								
						}
        try{
            res = new BDBOBStoreByteArray(name, env.openDatabase(null, name, dbConfig), env.openDatabase(null, name + "seq", dbConfig));
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
       return res;
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

<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
				public OBStore${Type} createOBStore${Type}(String name, boolean temp, boolean duplicates) throws OBStorageException{
        
        OBStore${Type} res = null;
        try{
            DatabaseConfig dbConfig = createDefaultDatabaseConfig();
						if(duplicates){
								dbConfig.setSortedDuplicates(true);
						}
            res = new BDBOBStore${Type}(name, env.openDatabase(null, name, dbConfig), env.openDatabase(null, name + "seq", dbConfig));
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
       return res;
    }
</#list>

}
