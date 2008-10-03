<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#include "/@inc/bdb.ftl">
<#list bdbs as b>
<@type_info_bdb b=b/>
<@pp.changeOutputFile name="BDBFactory${Bdb}.java" />
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
import java.io.FileNotFoundException;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OBException;

import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.constants.OBSearchProperties;

<#if bdb = "db">
	import	com.sleepycat.db.DatabaseType;
</#if>

<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
import net.obsearch.storage.OBStore${Type};
</#list>



import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.OBStoreFactory;

import com.sleepycat.${bdb}.Database;
import com.sleepycat.${bdb}.DatabaseConfig;
import com.sleepycat.${bdb}.DatabaseException;
import com.sleepycat.${bdb}.Environment;
import com.sleepycat.${bdb}.EnvironmentConfig;
import com.sleepycat.bind.tuple.*;
import com.sleepycat.je.DatabaseEntry;


import java.io.File;
import org.apache.log4j.Logger;

/**
 * BDBFactory generates an environment in the given directory, and creates
 * databases as OBSearch requests.
 * @author Arnoldo Jose Muller Molina
 */

public final class BDBFactory${Bdb} implements OBStoreFactory {
		/*
			<#list bdbs as b>
			  ${b.name}
			</#list>
     */
		/**
		 * Logger.
		 */
		private static final transient Logger logger = Logger
			.getLogger(BDBFactory${Bdb}.class);

		private String directory;

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
    public BDBFactory${Bdb}(File directory) throws IOException, DatabaseException 	<#if bdb = "db">, OBException </#if> {
				this.directory = directory.getAbsolutePath();
				logger.debug("Factory created on dir: " + directory);
        directory.mkdirs();
        OBAsserts.chkFileExists(directory);
        EnvironmentConfig envConfig = createEnvConfig();
        env = new Environment(directory, envConfig);
				if(logger.isDebugEnabled()){
								logger.debug("Environment config: \n" + env.getConfig().toString());
								<#if bdb = "je">
								logger.debug("Buffer size " + env.getConfig().getConfigParam("je.log.bufferSize"));
								logger.debug("Cache % " + env.getConfig().getConfigParam("je.maxMemoryPercent"));
								</#if>
								
				}
    }

		public String getFactoryLocation(){
				return directory;
		}
    
    /**
     * Creates the default environment configuration.
     * @return Default environment configuration.
     */
    private EnvironmentConfig createEnvConfig() 	<#if bdb = "db"> throws OBException </#if>{
        /* Open a transactional Oracle Berkeley DB Environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
				<#if bdb = "je">
        envConfig.setConfigParam("java.util.logging.DbLogHandler.on", "false");
				envConfig.setLocking(true);
				<#else>
			  envConfig.setInitializeCache(true);
				envConfig.setInitializeLocking(true);
				envConfig.setCacheSize(OBSearchProperties.getBDBCacheSize());
				envConfig.setCacheCount(2);
				envConfig.setInitializeLogging(false);
				
				</#if>
				envConfig.setTxnNoSync(true);
        //envConfig.setTxnWriteNoSync(true);
				// envConfig.setCachePercent(20);
				// 100 k gave the best performance in one thread and for 30 pivots of
        // shorts
				
				//						envConfig.setConfigParam("je.log.faultReadSize", "140000");	 
				//    envConfig.setConfigParam("je.evictor.lruOnly", "false");
        //    envConfig.setConfigParam("je.evictor.nodesPerScan", "100");
        return envConfig;
    }

    public void close() throws OBStorageException {
        try {
						<#if bdb = "je">
            env.cleanLog();
            env.compress();
            env.checkpoint(null);
						</#if>
 
            env.close();
        } catch (DatabaseException e) {
            throw new OBStorageException(e);
        }
    }

    public OBStore<TupleBytes> createOBStore(String name, OBStorageConfig config) throws OBStorageException{       

        OBStore res = null;
				
       
        try{

						<@prepareStorageDevice/>
        
						res = new BDBOBStore${Bdb}ByteArray(name, <@openDB/> , seq, this, duplicates);
					
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
				<#if bdb = "db">
				catch(FileNotFoundException e){
            throw new OBStorageException(e);
        }
				</#if>
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
				<#if bdb = "db">
	  		 // Using database mode: ${bdb_mode}
											 dbConfig.setType(DatabaseType.${bdb_mode});
				
				<#else>
						 
				</#if>
        return dbConfig;
    }

		public void removeOBStore(OBStore storage) throws OBStorageException{
						storage.close();
						try{
						<#if bdb = "je">
						env.removeDatabase(null, storage.getName());
						env.removeDatabase(null, sequentialDatabaseName(storage.getName()));
						<#else>
								 env.removeDatabase(null, storage.getName(), storage.getName());
								env.removeDatabase(null, sequentialDatabaseName(storage.getName()), sequentialDatabaseName(storage.getName()));
						</#if>
						}catch(DatabaseException e){
								throw new OBStorageException(e);
						}
							<#if bdb = "db">
				catch(FileNotFoundException e){
            throw new OBStorageException(e);
        }
				</#if>
				}

<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
				
				

				public OBStore${Type} createOBStore${Type}(String name, OBStorageConfig config) throws OBStorageException{
        
        OBStore${Type} res = null;
        try{
            
						<@prepareStorageDevice/>
        
						res = new BDBOBStore${Bdb}${Type}(name, <@openDB/> , seq, this, duplicates);
								
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
					<#if bdb = "db">
				catch(FileNotFoundException e){
            throw new OBStorageException(e);
        }
				</#if>
       return res;
    }

		
		public  byte[] serialize${Type}(${type} value){
				return BDBFactory${Bdb}.${type}ToBytes(value);
		}


		public ${type} deSerialize${Type}(byte[] value){
				DatabaseEntry entry = new DatabaseEntry(value);
				return ${binding}Binding.entryTo${Type}(entry);
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
				<#if bdb = "je">
				return	env.getStats(null);
				<#else>
						 return env.getLockStats(null);
				</#if>
		}catch(DatabaseException d){
				throw new OBStorageException(d);    
    }
}
}

</#list>