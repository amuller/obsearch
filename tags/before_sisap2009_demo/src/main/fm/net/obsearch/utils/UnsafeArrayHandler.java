<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="UnsafeArrayHandler"+Type+".java" />

package net.obsearch.utils;

import net.obsearch.index.utils.AbstractUnsafeArrayHandler;


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
* Class that allows the direct access of data from byte arrays.
* It uses the class UnSafe that is not part of the JDK.
* This is not a good thing to use 
* @author Arnoldo Jose Muller Molina
* @since 0.8
*/
public final class UnsafeArrayHandler${Type} extends AbstractUnsafeArrayHandler{        
    
    
    /**
     * The size in bytes of each entry of a ${type}.
     */
    public static int size = -1;
    
    
    public UnsafeArrayHandler${Type}(){
        super();
        if(size == -1){
            ${type}[] data = new ${type}[1];
            size = unsafe.arrayIndexScale(data.getClass());
        }
    }
  
    
    /**
     * Returns the ith ${type} of the given byte array.
		 * @param data The data that we want to read.
		 * @param i The index of the ${type} to be read.
     * @return The ith ${type} of the given byte array
     */
    public final ${type} get${type}(byte[] data , int i){
        return unsafe.get${Type}(data, (i * size) + offset );
    }


		/**
     * Sets the ith ${type} of the given byte array.		 
		 * @param data The data that we want to read.
		 * @param i The index of the data.
		 * @param value The value that will be set.
		 * @param extraOffset Bytes to be added to the "left" of the byte array.
     * @return Sets ith ${type} of the given byte array
     */
    public final void put${type}(byte[] data , int i, ${type} value){
        unsafe.put${Type}(data, (i * size) + offset, value);
    }

		/**
     * Returns the ith ${type} of the given byte array. Adds an arbitrary
     * number of bytes to the left of the index, in case that you have
     * some extra data on the "left". After these bytes, you can assume that
     * the rest of the data is a ${type} array.
		 * @param data The data that we want to read.
		 * @param i The index of the ${type} to be read.
		 * @param extraOffset Bytes to be added to the "left" of the byte array.
     * @return The ith ${type} of the given byte array
     */
    public final ${type} get${type}(byte[] data , int i, int extraOffset){
        return unsafe.get${Type}(data, (i * size) + offset + extraOffset );
    }

		/**
     * Sets the ith ${type} of the given byte array
		 * Adds an arbitrary number of bytes to the left of the index, in case that you have
     * some extra data on the "left". After these bytes, you can assume that
     * the rest of the data is a ${type} array.
		 * @param data The data that we want to read.
		 * @param i The index of the data.
		 * @param value The value that will be set.
		 * @param extraOffset Bytes to be added to the "left" of the byte array.
     * @return Sets ith ${type} of the given byte array
     */
    public final void put${type}(byte[] data , int i, int extraOffset, ${type} value){
        unsafe.put${Type}(data, (i * size) + offset + extraOffset, value);
    }
    
    
}

</#list>
