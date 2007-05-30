/**
 * 
 */
package org.ajmm.obsearch.index;

import java.io.InputStream;
import java.util.Properties;

import org.ajmm.obsearch.TestOB;

import junit.framework.TestCase;

/*
    OBSearch: a distributed similarity search engine
    This project is to similarity search what 'bit-torrent' is to downloads.
    Copyright (C)  2007 Arnoldo Jose Muller Molina

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.   
*/
/** 
	Perform different tests on the pyramid technique
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/

public class TestExtentedPyramidIndex extends TestOB {
    
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
       
    }

    /**
     * Test method for {@link org.ajmm.obsearch.index.AbstractPivotIndex#insertObjectInDatabase(org.ajmm.obsearch.OB, int, com.sleepycat.je.Database)}.
     */
    public void testInsertObjectInDatabase() {
        fail("Not yet implemented");
    }

}
