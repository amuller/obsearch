package net.obsearch.ambient;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.storage.OBStoreFactory;


import com.thoughtworks.xstream.XStream;

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
 * An Ambient is the class that surrounds the index, its metadata and the storage
 * devices. It helps to serialize the index and put the serialized data in an appropiate
 * location. This is how you use an ambient:
 * 1) Create the Index you want to use. 
 * 2) Create the Ambient that holds the storage device you want to use and pass
 *     the Index to it.
 * 3) Forget about the Ambient, when you want to freeze the index, use the Ambient
 *     to freeze the index so that metadata is stored properly.
 * 4) When you have closed the index and you want to use it back again, call the ambient
 *     and it will de-serialize the index and leave it ready for you to use it.
 * 
 *  Sub-classes of this class, create ambients  for different storage devices.
 * @author Arnoldo Jose Muller Molina
 * @param <I> The index that we want to create with this ambient.
 */

public abstract class AbstractAmbient<O extends OB,  I extends Index<O>> implements Ambient < O, I> {

    /**
     * Index that will be used.
     */
    private I index;
    
    /**
     * Directory in which everything will be stored.
     */
    private File directory;
    
    /**
     * Factory that creates storage devices for the index.
     */
    protected OBStoreFactory fact;
    
    
    /**
     * 
     * @param index An index whose isFrozen() == false.
     * @param directory The directory in which everything will be stored.
     * @throws AlreadyFrozenException if the given index is already frozen.
     */
    protected AbstractAmbient(I index, File directory) throws AlreadyFrozenException, FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        if(index.isFrozen()){
            throw new AlreadyFrozenException();
        }
        this.index = index;
        this.directory = directory;
        initIndex();
    }
    
    /**
     * Constructor to be used after a freeze took place.
     * @param directory We will load the database from here.
     * @throws FileNotFoundException if no metadata is found
     */
    protected AbstractAmbient(File directory) throws FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        XStream xstream = new XStream();
        FileInputStream fs = new FileInputStream(metadataFile(directory));
        BufferedInputStream bf = new BufferedInputStream(fs);
        index =  (I) xstream.fromXML(bf);
        this.directory = directory;
        initIndex();
    }
    
    /**
     * Initializes the factory of the given index.
     * @throws FileNotFoundException
     * @throws OBStorageException
     * @throws NotFrozenException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws OBException
     * @throws IOException
     */
    private void initIndex() throws FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        File dbDirectory = dataFile(directory);
        fact = createFactory(dbDirectory);
        dbDirectory.mkdirs();
        index.init(fact);
    }
    
    public void close() throws OBException{
    	index.close();
    }
    
    /* (non-Javadoc)
     * @see net.obsearch.ambient.Ambient#getIndex()
     */
    public I getIndex(){
        return index;
    }
    
    /* (non-Javadoc)
     * @see net.obsearch.ambient.Ambient#freeze()
     */
    public void freeze() throws IOException, AlreadyFrozenException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBStorageException, OutOfRangeException, OBException{
        index.freeze();
        // serialize the stream.
        XStream xstream = new XStream();
        FileOutputStream fs = new FileOutputStream( metadataFile(directory) );
        BufferedOutputStream bf = new BufferedOutputStream(fs);
        xstream.toXML(index, bf);
    }
    
    /**
     * Creates the metadata file name.
     */
    private File metadataFile(File directory){
        return new File(directory,
                METADATA_FILENAME);
    }
    
    /**
     * Creates the data folder name.
     * @param directory
     * @return The directory name where the data will be stored.
     */
    private File dataFile(File directory){
        return new File(directory, "data");
    }
    
    /**
     * Creates the factory in the given folder.
     * @param factoryDirectory The location where the factory will work.
     * @return The factory ready to work!
     * @throws OBStorageException if something goes wrong with the DB.
     */
    protected abstract OBStoreFactory createFactory(File factoryDirectory) throws OBStorageException;

	@Override
	public OBStoreFactory getFactory() {
		// TODO Auto-generated method stub
		return fact;
	}
    
    
}
