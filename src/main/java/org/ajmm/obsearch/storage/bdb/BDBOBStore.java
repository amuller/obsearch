package org.ajmm.obsearch.storage.bdb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.storage.OBStore;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;

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
 * BDBOBStore is a storage abstraction for Berkeley DB. It is designed to work
 * on byte array keys storing byte array values.
 * @author Arnoldo Jose Muller Molina
 */

public class BDBOBStore implements OBStore {
    

    /**
     * Berkeley DB database.
     */
    protected Database db;
    
    /**
     * Database for sequences.
     */
    protected Database sequence;
    
    /**
     * Sequence counter
     */
    protected Sequence counter;

    /**
     * Name of the database.
     */
    private String name;

    /**
     * If this storage system accepts duplicates or not.
     */
    private boolean duplicates;

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
    public BDBOBStore(String name, Database db, Database sequences) throws DatabaseException {
        this.db = db;
        this.name = name;
        this.duplicates = db.getConfig().getSortedDuplicates();
        this.sequence = sequences;
        // initialize sequences
        SequenceConfig config = new SequenceConfig();
        config.setAllowCreate(true);
        DatabaseEntry key =
            new DatabaseEntry((name + "_seq").getBytes());
        this.counter = sequence.openSequence(null, key, config);
    }

    public void close() throws OBStorageException {
        try {
            db.close();
            sequence.close();
        } catch (DatabaseException d) {
            throw new OBStorageException(d);
        }
    }

    public Result delete(byte[] key) throws OBStorageException {
        Result r = new Result();
        try {
            OperationStatus res = db.delete(null, new DatabaseEntry(key));
            if (res.NOTFOUND == res) {
                r.setStatus(Result.Status.NOT_EXISTS);
            } else if (res.SUCCESS == res) {
                r.setStatus(Result.Status.OK);
            } else {                
                assert false;
            }
        } catch (Exception e) {
            throw new OBStorageException(e);
        }
        return r;
    }

    public void deleteAll() throws OBStorageException {
        try {
            db.getEnvironment().truncateDatabase(null, getName(), false);
        } catch (DatabaseException d) {
            throw new OBStorageException(d);
        }
    }

    public String getName() {
        return this.name;
    }

    public byte[] getValue(byte[] key) throws IllegalArgumentException, OBStorageException{
        if(duplicates){
            throw new IllegalArgumentException();
        }
        DatabaseEntry search = new DatabaseEntry(key);
        DatabaseEntry value = new DatabaseEntry();
        try{
            OperationStatus res = db.get(null, search, value, null);
            if(res == OperationStatus.SUCCESS){
                return value.getData();
            }else{
                return null;
            }
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
    }

    public Result put(byte[] key, byte[] value) throws OBStorageException{
        
        DatabaseEntry k = new DatabaseEntry(key);
        DatabaseEntry v = new DatabaseEntry(value);
        Result res = new Result();
        try{            
            OperationStatus r = db.put(null, k, v);
            if(r == OperationStatus.SUCCESS){ 
                res.setStatus(Result.Status.OK);
            } // Result() is always initialized with error.
        }catch(DatabaseException e){
            throw new OBStorageException(e);
        }
        return res;
    }

   public boolean allowsDuplicatedData(){
       return duplicates;
   }
   
   /**
    * Base class used to iterate over cursors.
    * @param <O> The type of tuple that will be returned by the iterator.
    */
   protected abstract class CursorIterator<O> implements Iterator< O >{

    protected Cursor cursor;
    private boolean cursorClosed = false;
    protected DatabaseEntry keyEntry = new DatabaseEntry();
    protected DatabaseEntry dataEntry = new DatabaseEntry();
    protected OperationStatus retVal;

    protected void closeCursor() {
        try{
            synchronized(cursor){
                if(!cursorClosed){
                    cursor.close();
                    cursorClosed = true;
                }
            }
        }catch(DatabaseException e){
            throw new NoSuchElementException("Could not close the internal cursor");
        }
    }

    /**
     * Currently not supported. To be supported in the future.
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void finalize() throws Throwable {
        try{
            closeCursor();
        }finally{
            super.finalize();
        }
    }
       
   }
   
   
   
   public long size()  throws OBStorageException{
       long res;
       
       try{
           res = db.count();
       }catch(DatabaseException e){
           throw new OBStorageException(e);
       }
       return res;
   }
   
   /**
    * Returns the next id from the database (incrementing sequences). 
    * @return The next id that can be inserted. 
    */
   public long nextId() throws OBStorageException{
       long res ;
       try{
           res =  this.counter.get(null, 1);
       }catch(DatabaseException e){
           throw new OBStorageException(e);
       }
       return res;
   }

}
