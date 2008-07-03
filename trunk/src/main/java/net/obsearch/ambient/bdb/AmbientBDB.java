package net.obsearch.ambient.bdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.bdb.BDBFactory;

import net.obsearch.ambient.AbstractAmbient;

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
 * AmbientBDB creates an ambient based on the Berkeley DB storage sub-system.
 * @author Arnoldo Jose Muller Molina
 */

public class AmbientBDB<O extends OB,  I extends Index<O>> extends AbstractAmbient<O, I>{

    /**
     * @see org.ajmm.obsearch.ambient.AbstractAmbient#AbstractAmbient(I index, File directory)
     */
    public AmbientBDB(I index, File directory) throws FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        super(index,directory);
    }
    
    /**
     * @see org.ajmm.obsearch.ambient.AbstractAmbient#AbstractAmbient(File directory)
     */
    public AmbientBDB(File directory) throws FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        super(directory);
    }
    /* (non-Javadoc)
     * @see net.obsearch.ambient.AbstractAmbient#createFactory(java.io.File)
     */
    @Override
    protected OBStoreFactory createFactory(File factoryDirectory) throws OBStorageException{        
        BDBFactory fact = null;
        try{
            fact = new BDBFactory(factoryDirectory);
        }catch(Exception e){
            throw new OBStorageException(e);
        }
        return fact;
    }

}
