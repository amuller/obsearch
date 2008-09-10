<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="TestOBLStore${Type}.java" />
package net.obsearch.storage.l;

import java.io.File;

import junit.framework.TestCase;

import net.obsearch.storage.bdb.Utils;
import net.obsearch.index.utils.Directory;
import net.obsearch.storage.StorageValidation${Type};
import net.obsearch.storage.OBStorageConfig;
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
public class TestOBLStore${Type} extends TestCase{

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testAll() throws Exception{
        
         OBLFactory fact = Utils.getFactoryL();
				 OBStorageConfig conf = new OBStorageConfig();
				 conf.setTemp(false);
				 conf.setDuplicates(true);
				 conf.setBulkMode(false);
				 conf.setRecordSize(StorageValidation${Type}.STORAGE_SIZE);
				 StorageValidation${Type}.validateDuplicates(fact.createOBStore${Type}("test${Type}1", conf));
				    conf = new OBStorageConfig();
				 conf.setTemp(false);
				 conf.setDuplicates(true);
				 conf.setBulkMode(true);
				 conf.setRecordSize(StorageValidation${Type}.STORAGE_SIZE);
				 StorageValidation${Type}.validateDuplicates(fact.createOBStore${Type}("test${Type}2", conf));
				   conf = new OBStorageConfig();
				 conf.setTemp(true);
				 conf.setDuplicates(true);
				 conf.setBulkMode(false);
				 conf.setRecordSize(StorageValidation${Type}.STORAGE_SIZE);
				 StorageValidation${Type}.validateDuplicates(fact.createOBStore${Type}("test${Type}3", conf));
    }

}

</#list>
