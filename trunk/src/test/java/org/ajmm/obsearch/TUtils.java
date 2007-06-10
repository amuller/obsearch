/**
 *
 */
package org.ajmm.obsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;

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
 * Class: TestOB Test utils class
 *
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class TUtils {

	 	public static Properties testProperties = null;

        public static Properties getTestProperties() throws IOException{
        	if(testProperties == null){ // load the properties only once
        		 InputStream is = TUtils.class.getResourceAsStream( "/test.properties" );
                 testProperties = new Properties();
                 testProperties.load(is);
                 // configure log4j only once too
                 PropertyConfigurator.configure(testProperties
     					.getProperty("test.log4j.file"));
        	}

            return testProperties;
        }

    }

