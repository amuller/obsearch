package net.obsearch.ambient.tc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.ambient.AbstractAmbient;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.storage.tc.TCFactory;

public class AmbientTC<O extends OB,  I extends Index<O>> extends AbstractAmbient<O, I>  {
	/**
     * @see net.obsearch.result.ambient.AbstractAmbient#AbstractAmbient(I index, File directory)
     */
    public AmbientTC(I index, File directory) throws FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        super(index,directory);
    }
    
    /**
     * @see net.obsearch.result.ambient.AbstractAmbient#AbstractAmbient(File directory)
     */
    public AmbientTC(File directory) throws FileNotFoundException, OBStorageException,
    NotFrozenException, IllegalAccessException, InstantiationException,
    OBException, IOException{
        super(directory);
    }
    /* (non-Javadoc)
     * @see net.obsearch.ambient.AbstractAmbient#createFactory(java.io.File)
     */
    @Override
    protected TCFactory createFactory(File factoryDirectory) throws OBStorageException{        
        TCFactory fact = null;
        try{
            fact = new TCFactory(factoryDirectory);
        }catch(Exception e){
            throw new OBStorageException(e);
        }
        return fact;
    }
}
