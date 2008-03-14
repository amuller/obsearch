package org.ajmm.obsearch.index;

import java.io.File;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;

import com.sleepycat.je.DatabaseException;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * This class wraps an standard index, and allows the user to obtain information
 * regarding the most recent insertions and deletions. The idea is to used this
 * index in a distributed environment.
 * @param <O>
 *            The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class SynchronizableIndexShort < O extends OBShort >
        extends AbstractSynchronizableIndex < O > implements IndexShort < O > {

    /**
     * Internal index that actually stores the objects.
     */
    protected IndexShort < O > index;

    /**
     * Constructor.
     * @param source internal index
     * @param dbDir database were to put the sync index
     * @throws DatabaseException If something goes wrong with the DB
     */
    public SynchronizableIndexShort(IndexShort < O > source, File dbDir)
            throws DatabaseException {
        super(source, dbDir);
        this.index = source;
    }

    public int databaseSize() throws DatabaseException, OBStorageException {
        return this.getIndex().databaseSize();
    }

    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        index.searchOB(object, r, result);
    }
    
    public String getSerializedName(){
        return this.getClass().getSimpleName();
    }

    @Override
    public Index < O > getIndex() {
        return index;
    }

    
    public String toXML() {
        return getIndex().toXML();
    }

    public boolean intersects(O object, short r, int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        return index.intersects(object, r, box);
    }

    public int[] intersectingBoxes(O object, short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        return index.intersectingBoxes(object, r);
    }

    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        index.searchOB(object, r, result, boxes);
    }

}
