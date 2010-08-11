<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>

<@pp.changeOutputFile name="StorageValidation"+Type+".java" />
package net.obsearch.storage;

import net.obsearch.utils.bytes.ByteConversion;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import org.apache.log4j.Logger;

import junit.framework.TestCase;

import java.nio.ByteBuffer;

import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OBException;
import net.obsearch.storage.Tuple${Type};
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
		
		private static final transient Logger logger = Logger
			.getLogger(StorageValidation${Type}.class);
    
    /**
     * Create a vector of pairs of size {@link #NUM_OF_ITEMS} so that
     * we can test the storage sub-system. 
     */
    public static final int NUM_OF_ITEMS = 1000;

		public static final int STORAGE_SIZE = 8;
          
    /**
     * Validates a Storage for shorts. Makes sure that insertions and deletions and
     * the iterators are working well.      
     * @param storage
     * @throws OBStorageException
     */
    public static void validate(OBStore${Type} storage) throws OBStorageException, OBException{
        HashMap<${ClassType}, byte[]> testData = new HashMap<${ClassType}, byte[]>();
        Random x = new Random();
        int i = 0;
        ${type} min = ${ClassType}.MAX_VALUE;
        ${type} max = ${ClassType}.MIN_VALUE;
        while(i < NUM_OF_ITEMS){
						<#if type == "double" || type == "float" || type == "long" || 
                 type == "int">
														 ${type} ran = Math.abs(x.next${Type}());
            <#else>
            ${type} ran = (${type}) (x.nextInt() % ${Type}.MAX_VALUE);
						</#if>
            if(ran < min){
                min = ran;
            }
            if(ran > max){
                max = ran;
            }
						ByteBuffer buf = ByteConversion.createByteBuffer(8);
						buf.putDouble(x.nextDouble());
            testData.put(ran, buf.array());
            i++;
        }

				

        for(${type} j : testData.keySet()){
						//						logger.info(j +  ": " + Arrays.toString(testData.get(j)));
            storage.put(j, testData.get(j));
						//logger.info(Arrays.toString(testData.get(j)));
        }               
        // test that all the data is there:
        for(${type} j : testData.keySet()){               
						assertTrue(Arrays.equals(storage.getValue(j), testData.get(j)));
        }         

        
        // do a range search       
        CloseIterator<Tuple${Type}> it = storage.processRange(min, max);
        i = 0;
        boolean first = true;
        ${type} prev = ${ClassType}.MIN_VALUE;
        while(it.hasNext()){
            Tuple${Type} t = it.next();
						assertTrue(testData.get(t.getKey()) != null);
						assertTrue("A:" + Arrays.toString(testData.get(t.getKey())) +  "B:" + Arrays.toString(t.getValue()) + " i: " + i + "A-key: " + t.getKey() , Arrays.equals(testData.get(t.getKey()), t.getValue()) );
            //assertTrue(Arrays.equals(testData.get(t.getKey()), t.getValue().array()));
            if(first){
                prev = t.getKey();
                first = false;
            }else{
           
                assertTrue( "Prev: " + prev + " t: " + t.getKey() + " i: " + i, prev < t.getKey());
                prev = t.getKey();
            }
            i++;
        }
        assertEquals(testData.size(), i);
				it.closeCursor();
				// do a range search       
        it = storage.processRangeReverse(min, max);
        i = 0;
        first = true;
        prev = ${ClassType}.MIN_VALUE;
        while(it.hasNext()){
            Tuple${Type} t = it.next();
            assertTrue(Arrays.equals(testData.get(t.getKey()), t.getValue()));
            if(first){
                prev = t.getKey();
                first = false;
            }else{
           
                assertTrue( "Prev: " + prev + " t: " + t.getKey()+ " i: " + i, prev > t.getKey());
                prev = t.getKey();
            }
            i++;
        }
				it.closeCursor();
        assertEquals(testData.size(), i);


				
        // TODO: add more tests for the iterator
        // Test updates:
        for(${type} j : testData.keySet()){
            String d = x.nextDouble() + "";
            testData.put(j, d.getBytes());//:)
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


		/**
     * Validates a Storage for shorts. Makes sure that insertions and deletions and
     * the iterators are working well.      
     * @param storage
     * @throws OBStorageException
     */
    public static void validateDuplicates(OBStore${Type} storage) throws OBStorageException, OBException{
        HashMap<${ClassType}, byte[][]> testData = new HashMap<${ClassType}, byte[][]>();
        Random x = new Random();
        int i = 0;
        ${type} min = ${ClassType}.MAX_VALUE;
        ${type} max = ${ClassType}.MIN_VALUE;
        while(i < NUM_OF_ITEMS){
						<#if type == "double" || type == "float" || type == "long" || 
                 type == "int">
														 ${type} ran = Math.abs(x.next${Type}());
            <#else>
            ${type} ran = (${type}) (x.nextInt() % ${Type}.MAX_VALUE);
						</#if>
            if(ran < min){
                min = ran;
            }
            if(ran > max){
                max = ran;
            }
						byte[][] data = new byte[5][];
						int cx = 0;
						while(cx < data.length){
								ByteBuffer buf = ByteConversion.createByteBuffer(8);
								buf.putDouble(x.nextDouble());
								data[cx] = buf.array();
								cx++;
						}
						
            testData.put(ran, data);
            i++;
        }

				

        for(${type} j : testData.keySet()){
						//						logger.info(j +  ": " + Arrays.toString(testData.get(j)));
						byte[][] data = testData.get(j);
						for(byte[] d : data){
								storage.put(j, d);
						}           
						//logger.info(Arrays.toString(testData.get(j)));
        }               
        // test that all the data is there:
				
        for(${type} j : testData.keySet()){   
						for(byte[] d : testData.get(j)){
								CloseIterator<Tuple${Type}> it = storage.processRange(j,j);
								int found = 0;
								while(it.hasNext()){
										Tuple${Type} t = it.next();
										if(Arrays.equals(d, t.getValue())){
												found++;
										}
										
								}
								it.closeCursor();
								assertTrue(found == 1);
						}
        }      

				// test that the iteration proceeds normally.
				CloseIterator<Tuple${Type}> it = storage.processRange(${ClassType}.MIN_VALUE, ${ClassType}.MAX_VALUE);
				int c = 0;
				${type} key = -1;
					while(it.hasNext()){
							 Tuple${Type} t = it.next();
							 if(t.getKey() != key){
									 if(key != -1){
											 assert c == 5;
									 }
									 key = t.getKey();
									 c= 1;
							 }else{
									 c++;
							 }
					}
					it.closeCursor();



					// test that the iteration proceeds normally.
			 it = storage.processRangeReverse(${ClassType}.MIN_VALUE, ${ClassType}.MAX_VALUE);
				 c = 0;
				 key = -1;
					while(it.hasNext()){
							 Tuple${Type} t = it.next();
							 if(t.getKey() != key){
									 if(key != -1){
											 assert c == 5;
									 }
									 key = t.getKey();
									 c= 1;
							 }else{
									 c++;
							 }
					}
					it.closeCursor();

				// test  deletes:
				//logger.info("Testing Deletes");
					/*				for(${type} j : testData.keySet()){   
						for(byte[] d : testData.get(j)){
								 it = storage.processRange(j,j);
								//	logger.info("Begin cycle1");
								while(it.hasNext()){
										Tuple${Type} t = it.next();
										// delete this guy.
										it.remove();
										boolean found = false;
										CloseIterator<Tuple${Type}> it2 = storage.processRange(j,j);
										//	logger.info("Begin cycle2");
										while(it2.hasNext()){
												Tuple${Type} t2 = it2.next();
												found = Arrays.equals(t.getValue().array(), t2.getValue().array());
												if(found){
														// it should have been removed
														break;
												}
										}
										assertTrue(! found);
										it2.closeCursor();
								}
								it.closeCursor();
								
						}
						}*/
        
        
        
        
    }
    
   
}
</#list>
