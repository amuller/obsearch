<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="TestBDBOBStore"+Type+".java" />
package net.obsearch.storage.bdb;

import java.io.File;

import junit.framework.TestCase;


import net.obsearch.index.utils.Directory;
import net.obsearch.storage.StorageValidation${Type};
import org.junit.Before;
import org.junit.Test;
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
	*  TestBDBOBStore${Type} 
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */
public class TestBDBOBStore${Type} extends TestCase{

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testAll() throws Exception{
        
        BDBFactory fact = Utils.getFactory();
        StorageValidation${Type}.validate(fact.createOBStore${Type}("test${Type}", false,false, false));
				StorageValidation${Type}.validate(fact.createOBStore${Type}("test${Type}", false,false, true));

				StorageValidation${Type}.validate(fact.createOBStore${Type}("test${Type}", true,false, false));
    }

}
</#list>
