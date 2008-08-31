<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
<@pp.changeOutputFile name="BDBOBStore"+Type+".java" />
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import net.obsearch.storage.CloseIterator;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.OBStore${Type};
import net.obsearch.storage.Tuple${Type};

import com.sleepycat.bind.tuple.${binding}Binding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleInput;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import java.nio.ByteBuffer;

/** 
	*  BDBOBStore${Type} is a wrapper for Berkeley indexes that assumes
	*  that keys are ${type}s and values are byte[].
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */

public final class BDBOBStore${Type}
        extends AbstractBDBOBStore<Tuple${Type}> implements OBStore${Type} {

    /**
     * Builds a new Storage system by receiving a Berkeley DB database that uses
     * ${type}s as a primary indexing method.
     * @param db
     *                The database to be stored.
		 * @param seq     Sequences database.
     * @param name
     *                Name of the database.
     * @throws DatabaseException
     *                 if something goes wrong with the database.
     */
    public BDBOBStore${Type}(String name, Database db, Database seq) throws DatabaseException {
        super(name, db, seq);
    }

    public net.obsearch.OperationStatus delete(${type} key) throws OBStorageException {
        return super.delete(getBytes(key));
    }

    /**
     * Converts the given value to an array of bytes.
     * @param value
     *                the value to be converted.
     * @return An array of bytes with the given value encoded.
     */
    private byte[] getBytes(${type} value) {
        return BDBFactory.${type}ToBytes(value);
    }


		/**
     * Converts the value of the given entry into its primitive type.
     * @param entry
     *                The place where we will put the entry.
     */
    private ${type} bytesToValue(byte[] entry) {
        TupleInput in = new TupleInput(entry);
        return in.read${binding2}();
    }

    public ByteBuffer getValue(${type} key) throws IllegalArgumentException,
            OBStorageException {
        return super.getValue(getBytes(key));
    }

    public net.obsearch.OperationStatus put(${type} key, ByteBuffer value) throws IllegalArgumentException,
            OBStorageException {
        return super.put(getBytes(key), value);
    }

    public CloseIterator < Tuple${Type} > processRange(${type} low, ${type} high)
            throws OBStorageException {
        return new ${Type}Iterator(low, high);
    }

		public CloseIterator < Tuple${Type} > processRangeReverse(${type} low, ${type} high)
            throws OBStorageException {
        return new ${Type}Iterator(low, high,true);
    }

		public CloseIterator < Tuple${Type} > processAll()
            throws OBStorageException {
        return new ${Type}Iterator();
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
        
       
        private ${Type}Iterator(${type} min, ${type} max) throws OBStorageException {
						super(getBytes(min), getBytes(max));
        }

				/**
				 * Creates a new ${Type}Iterator given a min range, max range and
				 * a flag saying if this iterator will go "forward" or "backwards"
				 */
				private ${Type}Iterator(${type} min, ${type} max, boolean reverseMode) throws OBStorageException {
						super(getBytes(min), getBytes(max), false, reverseMode);
        }

				private ${Type}Iterator() throws OBStorageException {
						super(null, null,true, false);
        }

				protected Tuple${Type} createTuple(byte[] key, ByteBuffer value) {
            return new Tuple${Type}(bytesToValue(key),value);
        }
    }
}


</#list>