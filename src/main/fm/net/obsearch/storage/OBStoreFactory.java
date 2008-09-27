<#include "/@inc/ob.ftl">
package net.obsearch.storage;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OBException;
import net.obsearch.storage.OBStorageConfig;
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

/**
 * OBStoreFactory defines several methods to facilitate the creation of indexes
 * by OBSearch's indexes. In general, each index should receive an object that
 * implements OBStoreFactory and from then, the index will get databases from
 * the factory as needed. Constructors for each factory are expected to define
 * how and where the data will be accessed. If the factory cannot provide some
 * of the requested indexes, an
 * {@link #net.obsearch.exception.UnsupportedStorageException} is thrown.
 * @author Arnoldo Jose Muller Molina
 */

public interface OBStoreFactory {

    /**
     * Creates a generic OBStore.
     * @param temp
     *                If true, the database will be configured to be a temporal
     *                database.
     * @param name The name of the database.
		 * @param duplicates If duplicates are to be allowed.
		 * @param bulkMode If we want the storage to be faster for lots of insertions.
     * @return An OBStore ready to be used.
     * @throws OBStorageException If the DB cannot be created.
     */
    OBStore<TupleBytes> createOBStore(String name, OBStorageConfig config) throws OBStorageException, OBException;
    
		/**
     * Removes all indexes and structures related to the given storage device.
		 * @param storage The storage device to remove.
		 * @throws OBStorageException If something goes wrong with the delete operation.
		 */
		void removeOBStore(OBStore storage) throws OBStorageException;	

    
<#list types as t>
<@type_info t=t/>
    /**
     * Creates an OBStore${Type} whose key is based on ${type}s.
     * @param name The name of the database.
     * @param temp
     *                If true, the database will be configured to be a temporal
     *                database.
     * @param duplicates If duplicates are to be allowed.
		 * @param bulkMode If we want the storage to be faster for lots of insertions.
     * @return An OBStore${Type} ready to be used.
     * @throws OBStorageException If the DB cannot be created.
     */
				OBStore${Type} createOBStore${Type}(String name, OBStorageConfig config) throws OBStorageException, OBException;	


		
		

    /**
     * Creates a binary representation of the given value.
     * The value must be returned in such a way that it can be sorted
     * with bytes.
		 */
		byte[] serialize${Type}(${type} value);


		/**
     * Creates a ${type} of the given value.
		 * Only the required bytes are taken from the input.
		 */
		 ${type} deSerialize${Type}(byte[] value);

</#list>
    
    /** 
     * Close the factory. All opened OBStores must be closed before this
     * method is called.
     * @throws OBStorageException if something goes wrong with the
     * underlying storage system.
     */ 
    void close() throws OBStorageException;


		/**
     * Return a stats object that is to be printed
     * @throws OBStorageException if something goes wrong with the
     * underlying storage system.
     */
		Object stats() throws OBStorageException;
}
