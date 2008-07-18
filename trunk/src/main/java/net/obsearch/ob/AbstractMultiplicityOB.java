package net.obsearch.ob;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import net.obsearch.OB;

import org.ajmm.obsearch.exception.OBException;

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
 * AbstractOB is used as an example on how to add extra information like
 * multiplicity, etc. You can inherit from this class if you wish to keep track
 * of the multiplicity of this 
 * @author Arnoldo Jose Muller Molina
 */

public abstract class AbstractMultiplicityOB implements OB, MultiplicityAware {
    private long multiplicity;

    /**
     * This method is only for you as OBSearch does not care about
     * multiplicity
     * @return the multiplicity
     */
    public long getMultiplicity() {
        return multiplicity;
    }
    
    public void incrementMultiplicity(){
        multiplicity++;
    }
    
    public void decrementMultiplicity(){
        multiplicity--;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Storable#load(java.io.DataInputStream)
     */
    @Override
    public void load(DataInputStream in) throws IOException, OBException {
        multiplicity = in.readLong();
        loadAux(in);
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Storable#store(java.io.DataOutputStream)
     */
    @Override
    public void store(DataOutputStream out) throws IOException{
        out.writeLong(multiplicity);
        storeAux(out);
    }
    
    /**
     * Stores this object in a byte array.
     * @param out
     *            A DataOutputStream where values can be stored
     * @since 0.0
     */
    protected abstract void storeAux(DataOutputStream out) throws IOException;

    /**
     * Populates the object's internal properties from the given byte stream.
     * @param in
     *            A DataInputStream object from where primitive types can be loaded.
     * @throws OBException
     *             if the data cannot be loaded.
     * @since 0.0
     */
    protected abstract void loadAux(DataInputStream in) throws OBException;
    
    
    
}
