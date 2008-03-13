<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<#if type == "int">
<#assign binding = "Integer">
<#assign binding2 = "Int">
<#elseif type == "float">
<#assign binding = "SortedFloat">
<#assign binding2 = binding>
<#elseif type == "double">
<#assign binding = "SortedDouble">
<#assign binding2 = binding>
<#else>
<#assign binding = Type>
<#assign binding2 = binding>
</#if>



<@pp.changeOutputFile name="BDBOBStore"+Type+".java" />
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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.storage.OBStore${Type};
import org.ajmm.obsearch.storage.Tuple${Type};

import com.sleepycat.bind.tuple.${binding}Binding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

/** 
	*  BDBOBStore${Type} is a wrapper for Berkeley indexes that assumes
	*  that keys are ${type}s and values are byte[].
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */

public final class BDBOBStore${Type}
        extends BDBOBStore implements OBStore${Type} {

    /**
     * Builds a new Storage system by receiving a Berkeley DB database that uses
     * ${type}s as a primary indexing method.
     * @param db
     *                The database to be stored.
     * @param name
     *                Name of the database.
     * @throws DatabaseException
     *                 if something goes wrong with the database.
     */
    public BDBOBStore${Type}(String name, Database db) throws DatabaseException {
        super(name, db);
    }

    public Result delete(${type} key) throws OBStorageException {
        return super.delete(getBytes(key));
    }

    /**
     * Converts the given value to an array of bytes.
     * @param value
     *                the value to be converted.
     * @return An array of bytes with the given value encoded.
     */
    private byte[] getBytes(${type} value) {
        TupleOutput out = new TupleOutput();
        out.write${binding2}(value);
        return out.getBufferBytes();
    }

    /**
     * Loads the given value to a DatabaseEntry entry
     * @param value
     *                The value to load
     * @param entry
     *                The place where we will put the entry.
     */
    private void loadIntoEntry(${type} value, DatabaseEntry entry) {
				entry.setData(getBytes(value));
    }

    /**
     * Converts the value of the given entry into its primitive type.
     * @param entry
     *                The place where we will put the entry.
     */
    private ${type} entryToValue(DatabaseEntry entry) {
        return ${binding}Binding.entryTo${Type}(entry);
    }

    public byte[] getValue(${type} key) throws IllegalArgumentException,
            OBStorageException {
        return super.getValue(getBytes(key));
    }

    public Result put(${type} key, byte[] value) throws IllegalArgumentException,
            OBStorageException {
        return super.put(getBytes(key), value);
    }

    public Iterator < Tuple${Type} > processRange(${type} low, ${type} high)
            throws OBStorageException {
        return new ${Type}Iterator(low, high);
    }

    /**
     * Iterator used to process range results.
     */
    /*
     * TODO: I am leaving the closing of the cursor to the last iteration or the
     * finalize method (whichever happens first). We should test if
     * this is ok, or if there is an issue with this because
     * Berkeley's iterator explicitly have a "close" method.
     */
    final class ${Type}Iterator extends CursorIterator < Tuple${Type} > {
        
        private Tuple${Type} next = null;

        private ${type} max;

        private ${type} current;

        private ${Type}Iterator(${type} min, ${type} max) throws OBStorageException {
            this.max = max;
            this.current = min;
            try {
                this.cursor = db.openCursor(null, null);
                loadIntoEntry(current, keyEntry);
                retVal = cursor.getSearchKeyRange(keyEntry, dataEntry, null);
            } catch (DatabaseException e) {
                throw new OBStorageException(e);
            }
            loadNext();
        }

        public boolean hasNext() {
            return next != null;
        }

        /**
         * Loads data from keyEntry and dataEntry and puts it into next. If we
         * go beyond max, we set next to null so that everybody will work
         * properly.
         */
        private void loadNext() throws NoSuchElementException {
            if (retVal == OperationStatus.SUCCESS) {
                current = entryToValue(keyEntry);
                if (current <= max) {
                    next = new Tuple${Type}(current, dataEntry.getData());
                } else { // end of the loop
                    next = null;
                    // close the cursor
                    closeCursor();
                }
            } else { // we are done
                next = null;
                // close the cursor
                closeCursor();
            }
        }
        
        public Tuple${Type} next() {
            synchronized (keyEntry) {
                if (next == null) {
                    throw new NoSuchElementException(
                            "You tried to access an iterator with no next elements");
                }
                Tuple${Type} res = next;
                try {
                    retVal = cursor.getNext(keyEntry, dataEntry, null);
                } catch (DatabaseException e) {
                    throw new NoSuchElementException("Berkeley DB's error: "
                            + e.getMessage());
                }
                // get the next elements.
                loadNext();
                return res;
            }
        }
    }
}


</#list>
