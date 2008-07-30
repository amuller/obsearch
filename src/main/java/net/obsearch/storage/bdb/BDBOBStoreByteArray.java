package net.obsearch.storage.bdb;

import java.util.Iterator;

import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.bdb.AbstractBDBOBStore.ByteArrayIterator;
import net.obsearch.storage.bdb.AbstractBDBOBStore.CursorIterator;


import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;

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
 * BDBOBStoreByteArray. This class makes the implementation of
 * BDBOBStore much cleaner and simpler. 
 * @author Arnoldo Jose Muller Molina
 */

public class BDBOBStoreByteArray
        extends AbstractBDBOBStore < TupleBytes > {
    /**
     * Builds a new Storage system by receiving a Berkeley DB database.
     * @param db
     *                The database to be stored.
     * @param name
     *                Name of the database.
     * @param sequences
     *                Database used to store sequences.
     * @throws DatabaseException
     *                 if something goes wrong with the database.
     */
    public BDBOBStoreByteArray(String name, Database db, Database sequences)
            throws DatabaseException {
        super(name, db, sequences);
    }
    
    

    @Override
    public CloseIterator < TupleBytes > processAll() throws OBStorageException {
        return new ByteArrayIterator();
    }
    
    

}
