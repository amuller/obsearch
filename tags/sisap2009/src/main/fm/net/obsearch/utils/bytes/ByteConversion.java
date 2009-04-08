<#include "/@inc/ob.ftl">
package net.obsearch.utils.bytes;

import java.nio.ByteBuffer;
import net.obsearch.constants.ByteConstants;
import java.nio.ByteOrder;
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
 * ByteConversion. Utilities to convert primitive types from and to byte arrays.
 * @author Arnoldo Jose Muller Molina
 */

public class ByteConversion {
		
		public static final ByteOrder ORDERING = ByteOrder.nativeOrder();
    /**
     * Create a ByteBuffer of size n.
     * @param n size of the new buffer. 
     * @return The buffer.
     */
    public static ByteBuffer createByteBuffer(int n){
        byte [] r = new byte[n];        
        return createByteBuffer(r);
    }
    
    /**
     * Create a ByteBuffer from the given byte array.
     * @param n size of the new buffer. 
     * @return The buffer.
     */
    public static ByteBuffer createByteBuffer(byte[] data){        
        ByteBuffer res = ByteBuffer.wrap(data);
				res.order(ORDERING);
        return res;
    }
		
		<#list types as t>
		<@type_info t=t/>
				
    /**
     * Reads a(n) ${Type} from the beginning of the given data array.
     * No size checks are performed.
     * @param data  Data that holds the encoded ${type}.
     * @return ${type} parsed from data.
     */
				public static ${type} bytesTo${Type}(byte[] data){
						<#if type == "byte">
            return createByteBuffer(data).get();        
						<#else>
						return createByteBuffer(data).get${Type}();        
				    </#if>
    }
    
    /**
     * Convert a(n) ${Type} into bytes.
     * @param i value to convert.
     * @return The byte array that represents the value.
     */
  	public static byte[] ${type}ToBytes(${type} i){
        byte [] res = new byte[ByteConstants.${Type}.getSize()];
				<#if type == "byte">
				createByteBuffer(res).put(i);
        <#else>
        createByteBuffer(res).put${Type}(i);
				</#if>
        return res;
    }

		/**
     * Reads a(n) ${Type} from the beginning of the given ByteBuffer.
     * No size checks are performed.
     * @param data  Data that holds the encoded ${type}.
     * @return ${type} parsed from data.
     */
				public static ${type} byteBufferTo${Type}(ByteBuffer data){
						<#if type == "byte">
            return data.get();        
						<#else>
						return data.get${Type}();        
				    </#if>
    }
    
    /**
     * Convert a(n) ${Type} into bytes.
     * @param i value to convert.
     * @return The byte array that represents the value.
     */
  	public static ByteBuffer ${type}ToByteBuffer(${type} i){
        ByteBuffer res = createByteBuffer(ByteConstants.${Type}.getSize());
				<#if type == "byte">
				res.put(i);
        <#else>
        res.put${Type}(i);
				</#if>
        return res;
    }

		</#list>
    
    
}
