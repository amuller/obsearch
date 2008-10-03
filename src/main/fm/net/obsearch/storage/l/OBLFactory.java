<#include "/@inc/ob.ftl">
package net.obsearch.storage.l;
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
import net.obsearch.storage.bdb.BDBFactoryJe;
import com.sleepycat.je.DatabaseException;

import net.obsearch.storage.OBStorageConfig;

import net.obsearch.storage.OBStore;

<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
import net.obsearch.storage.OBStore${Type};
</#list>



import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.OBStoreFactory;


import java.io.File;
import org.apache.log4j.Logger;

/**
 * BDBFactory generates an environment in the given directory, and creates
 * databases as OBSearch requests.
 * @author Arnoldo Jose Muller Molina
 */

public class OBLFactory implements OBStoreFactory {
		/*
			<#list bdbs as b>
			  ${b.name}
			</#list>
     */
		/**
		 * Logger.
		 */
		private static final transient Logger logger = Logger
			.getLogger(OBLFactory.class);

    /**
     * The environment.
     */
    private BDBFactoryJe baseFactory;

		private File baseDirectory;

		/**
		 * Root dir of all the RandomAccessFiles.
		 */
		private File baseLDirectory;

		

    /**
     * Creates a new factory that will be based in the given directory.
     * @param directory
     *                The directory where the Berkeley DB files will be stored.
     * @throws IOException
     *                 If the given directory does not exist.
     */
    public OBLFactory(File directory) throws IOException,  DatabaseException {
        directory.mkdirs();
        OBAsserts.chkFileExists(directory);
        this.baseDirectory = directory;
				baseFactory = new BDBFactoryJe(new File(baseDirectory, "storages"));
				baseLDirectory = new File(baseDirectory, "L");
    }
    
    

    public void close() throws OBStorageException {
        baseFactory.close();
    }

    public OBStore<TupleBytes> createOBStore(String name, OBStorageConfig config) throws OBStorageException, OBException{      
				boolean temp = config.isTemp();
				boolean bulkMode = config.isBulkMode();
				boolean duplicates = config.isDuplicates();
				int recordSize = config.getRecordSize();
			

				if(duplicates){
						config.setDuplicates(false);
						return new OBLStoreByteArray(name, baseFactory.createOBStore(name, config), this, duplicates, recordSize,  baseLDirectory );
				}else{
						return baseFactory.createOBStore(name, config);
				}
        
    }

	
    
    

		public void removeOBStore(OBStore storage) throws OBStorageException{
				baseFactory.removeOBStore(storage);
				
		}

<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
				
				

				public OBStore${Type} createOBStore${Type}(String name, OBStorageConfig config) throws OBStorageException, OBException{
        
				boolean temp = config.isTemp();
				boolean bulkMode = config.isBulkMode();
				boolean duplicates = config.isDuplicates();
				int recordSize = config.getRecordSize();
        
        if(duplicates){
						config.setDuplicates(false);
						return new OBLStore${Type}(name, baseFactory.createOBStore${Type}(name, config), this , duplicates, recordSize, baseLDirectory );
				}else{
						return baseFactory.createOBStore${Type}(name, config);
				}
       
    }

		
		public  byte[] serialize${Type}(${type} value){
				return baseFactory.${type}ToBytes(value);
		}

		public ${type}  deSerialize${Type}(byte[] value){
				return baseFactory.deSerialize${Type}(value);
		}

		

	

</#list>

		public	String getFactoryLocation(){
				return baseFactory.getFactoryLocation();
		}
		
		public Object stats() throws OBStorageException{
		return new Object(); // TODO: add stats.
}
}

