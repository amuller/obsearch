/**
 * 
 */
package org.ajmm.obsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
	  Class: TestOB
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/

public class TestOB extends TestCase {
    Properties testProperties; // properties required during the test
    
    public TestOB() {
        fail("Default constructor ha dame");
    }
    
    /**
     * @param name the name of the test
     */
    public TestOB(String name) throws IOException{
        super(name);
        
        InputStream is = getClass().getResourceAsStream( "/test.properties" );
        testProperties.load(is);
    }

   

}
