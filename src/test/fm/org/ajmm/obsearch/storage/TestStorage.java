<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>

<#if type == "int">
<#assign Type2 = "Integer">
<#else>
<#assign Type2 = t.name?cap_first>
</#if>

<#assign Type = t.name?cap_first>

<@pp.changeOutputFile name="StorageValidation"+Type+".java" />
package org.ajmm.obsearch.storage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.storage.Tuple${Type};
/*
OBSearch: a distributed similarity search engine
This project is to similarity search what 'bit-torrent' is to downloads.
Copyright (C)  2008 Arnoldo Jose Muller Molina

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
public class StorageValidation${Type} extends TestCase {
    
    /**
     * Create a vector of pairs of size {@link #NUM_OF_ITEMS} so that
     * we can test the storage sub-system. 
     */
    public static final int NUM_OF_ITEMS = 2000;
          
    /**
     * Validates a Storage for shorts. Makes sure that insertions and deletions and
     * the iterators are working well.      
     * @param storage
     * @throws OBStorageException
     */
    public static void validate(OBStore${Type} storage) throws OBStorageException{
        HashMap<${Type2}, byte[]> testData = new HashMap<${Type2}, byte[]>();
        Random x = new Random();
        int i = 0;
        ${type} min = ${Type2}.MAX_VALUE;
        ${type} max = ${Type2}.MIN_VALUE;
        while(i < NUM_OF_ITEMS){
						<#if type == "double" || type == "float" || type == "long" || 
                 type == "int">
            ${type} ran = x.next${Type}();
            <#else>
            ${type} ran = (${type}) (x.nextInt() % ${Type}.MAX_VALUE);
						</#if>
            if(ran < min){
                min = ran;
            }
            if(ran > max){
                max = ran;
            }
            String d = x.nextDouble() + "";
            testData.put(ran, d.getBytes());
            i++;
        }

        for(${type} j : testData.keySet()){
            storage.put(j, testData.get(j));
        }               
        // test that all the data is there:
        for(${type} j : testData.keySet()){               
             assertTrue(Arrays.equals(storage.getValue(j), testData.get(j)));
        }         
        
        // do a range search       
        Iterator<Tuple${Type}> it = storage.processRange(min, max);
        i = 0;
        boolean first = true;
        ${type} prev = ${Type2}.MIN_VALUE;
        while(it.hasNext()){
            Tuple${Type} t = it.next();
            assertTrue(Arrays.equals(testData.get(t.getKey()), t.getValue()));
            if(first){
                prev = t.getKey();
                first = false;
            }else{
           
                assertTrue( "Prev: " + prev + " t: " + t.getKey(), prev < t.getKey());
                prev = t.getKey();
            }
            i++;
        }
        assertEquals(testData.size(), i);
        // TODO: add more tests for the iterator
        
        // Test updates:
        for(${type} j : testData.keySet()){
            String d = x.nextDouble() + "";
            testData.put(j,d.getBytes());
            storage.put(j, d.getBytes());
        }          
        // test that all the new  data is there:
        for(${type} j : testData.keySet()){               
             assertTrue(Arrays.equals(storage.getValue(j), testData.get(j)));
        }    
        
        // Test deletes:
        for(${type} j : testData.keySet()){
            assertTrue( storage.getValue(j) != null);
            storage.delete(j);
            assertTrue(storage.getValue(j) == null);
        }
    }
    
   
}
</#list>
