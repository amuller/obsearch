package net.obsearch.ambient;

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
 *  Ambient is used to manage the serialization and de-serialization of indexes.
 *  You could do this by yourself but it is much more convenient to let Ambient
 *  do the work for you.
 *  @see org.obsearch.ambient.AbstractAmbient
 *  
 *  @author  Arnoldo Jose Muller Molina    
 */

import java.io.IOException;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.storage.OBStoreFactory;


public interface Ambient < O extends OB, I extends Index < O >> {
    
    /**
     * The name of the file in which the meta-data is stored.
     */
    String METADATA_FILENAME = "ob.xml";
    

    /**
     * Returns the index.
     * @return the index.
     */
    public abstract I getIndex();

    /**
     * Freezes the index and stores the metadata of the index (the index itself)
     * in the DB.
     * @throws PivotsUnavailableException 
     * @see net.obsearch.Index#freeze()
     */
    public abstract void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBStorageException, OutOfRangeException, OBException, PivotsUnavailableException;
    
    /**
     * Close all the internally used resources.
     * @throws OBException
     */
    public void close() throws OBException;
    
    /**
     * Returns the factory (if the user wants to create some
     * storages for personal use).
     * @return The factory used to create storage devices.
     */
    public OBStoreFactory getFactory();

}