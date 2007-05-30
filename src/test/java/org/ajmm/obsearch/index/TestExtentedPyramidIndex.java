/**
 * 
 */
package org.ajmm.obsearch.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.ajmm.obsearch.OBPriorityQueue;
import org.ajmm.obsearch.OBResult;
import org.ajmm.obsearch.TestOB;
import org.ajmm.obsearch.dimension.ShortDim;
import org.ajmm.obsearch.index.pivotselection.RandomPivotSelector;
import org.ajmm.obsearch.testutils.OBSlice;

import junit.framework.TestCase;

/*
 * OBSearch: a distributed similarity search engine This project is to
 * similarity search what 'bit-torrent' is to downloads. Copyright (C) 2007
 * Arnoldo Jose Muller Molina
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/**
 * Perform different tests on the pyramid technique
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class TestExtentedPyramidIndex extends TestOB {

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();

    }

    /**
     * Test method for
     * {@link org.ajmm.obsearch.index.AbstractPivotIndex#insertObjectInDatabase(org.ajmm.obsearch.OB, int, com.sleepycat.je.Database)}.
     * Creates a database, fills it with a bunch of rows performs a query and
     * compares the result with the sequential search.
     */
    public void testFull() throws Exception{
        File query = new File(testProperties.getProperty("test.query.input"));
        File db = new File(testProperties.getProperty("test.db.input"));
        String dbFolder = testProperties.getProperty("project.build.testOutputDirectory");
        ExtendedPyramidIndex<OBSlice, ShortDim> index = new ExtendedPyramidIndex<OBSlice, ShortDim>(new File(dbFolder), (byte)30, new ShortDim((short)0), new ShortDim((short)10000));
        // 
        BufferedReader r = new BufferedReader(new FileReader(db));
        String re = r.readLine();
        int realIndex = 0;

        while(re != null){
            String line = parseLine(re);
            if(line != null){
                index.insert(new OBSlice(line), realIndex);
            }
            re = r.readLine();
            realIndex++;
        }
        // we select the pivots and put all the stuff
        // in the database
        // the pyramid values are created
        index.freeze(new RandomPivotSelector());
        byte k = 3;
        ShortDim range = new ShortDim((short)3); // range
        // it is time to Search
        r = new BufferedReader(new FileReader(db));
        List<OBPriorityQueue<OBResult<OBSlice, ShortDim>, ShortDim>> result = new  LinkedList<OBPriorityQueue<OBResult<OBSlice, ShortDim>, ShortDim>>();
         re = r.readLine();        

        while(re != null){
            String line = parseLine(re);
            if(line != null){
                OBPriorityQueue<OBResult<OBSlice, ShortDim>, ShortDim> x = new  OBPriorityQueue<OBResult<OBSlice, ShortDim>, ShortDim>(k);                
                index.searchOB(new OBSlice(line), range, x);
                result.add(x);
            }
            re = r.readLine();          
        }
        
        // search is finished.
        // now we will match by using pivots only, using all the tuples
        
        
    }

    public String parseLine(String line) {
        if (line.startsWith("//") || line.trim().equals("")
                || (line.startsWith("#") && !line.startsWith("#("))) {
            return null;
        } else {
            String arr[] = line.split("[:]");
            if (arr.length == 2) {
                return arr[1];
            } else if (arr.length == 1) {
                return arr[0];
            } else {
                assert false : "Received line: " + line;
                return null;
            }
        }
    }

}
