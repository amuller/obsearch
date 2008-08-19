<#include "/@inc/ob.ftl">
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
<@type_info t=t/>
<@binding_info t=t/>
import net.obsearch.storage.OBStore${Type};
</#list>



import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.OBStoreFactory;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.bind.tuple.*;
import com.sleepycat.je.DatabaseEntry;

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
				envConfig.setTxnNoSync(true);
        //envConfig.setTxnWriteNoSync(true);
				//envConfig.setCachePercent(90);
				// 100 k gave the best performance in one thread and for 30 pivots of
        // shorts
				
				//		envConfig.setConfigParam("je.log.faultReadSize", "140000");	 
				//    envConfig.setConfigParam("je.evictor.lruOnly", "false");
        //    envConfig.setConfigParam("je.evictor.nodesPerScan", "100");
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
				
					dbConfig.setSortedDuplicates(duplicates);								
					dbConfig.setTemporary(temp);
					dbConfig.setTransactional(false);
        try{
            res = new BDBOBStoreByteArray(name, env.openDatabase(null, name, dbConfig), env.openDatabase(null, name + "seq", dbConfig));
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
       return res;
    }

		/**
	 * Generate the name of the sequential database based on name.
	 * @param name The name of the database.
	 * @return The sequential database name.
	 */
	private String sequentialDatabaseName(String name){
		return name + "seq";
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

		public void removeOBStore(OBStore storage) throws OBStorageException{
						storage.close();
						try{
						env.removeDatabase(null, storage.getName());
						env.removeDatabase(null, sequentialDatabaseName(storage.getName()));
						}catch(DatabaseException e){
								throw new OBStorageException(e);
						}
				}

<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
				
				

				public OBStore${Type} createOBStore${Type}(String name, boolean temp, boolean duplicates) throws OBStorageException{
        
        OBStore${Type} res = null;
        try{
            DatabaseConfig dbConfig = createDefaultDatabaseConfig();
						
								dbConfig.setSortedDuplicates(duplicates);
								dbConfig.setTemporary(temp);
            res = new BDBOBStore${Type}(name, env.openDatabase(null, name, dbConfig), env.openDatabase(null, sequentialDatabaseName(name), dbConfig));
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
       return res;
    }

		
		public  byte[] serialize${Type}(${type} value){
				return BDBFactory.${type}ToBytes(value);
		}

		public static byte[] ${type}ToBytes(${type} value){
				DatabaseEntry entry = new DatabaseEntry();
				${binding}Binding.${type}ToEntry(value, entry);
				assert entry.getData().length == net.obsearch.constants.ByteConstants.${Type}.getSize();
				return entry.getData();
		}

</#list>
		
		public Object stats() throws OBStorageException{
		try{
				return	env.getStats(null);
		}catch(DatabaseException d){
				throw new OBStorageException(d);    
    }
}
}
