<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="BucketContainer${Type}.java" />
package net.obsearch.index.bucket.impl;
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


import java.nio.ByteBuffer;
import net.obsearch.Index;
import net.obsearch.ob.OB${Type};
import java.lang.reflect.Array;
import net.obsearch.storage.OBStore${Type};
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.OBStore;
/** 
	*  BucketContainer is a dummy class that is used to directly
  *  inherit the functionality from AbstractBucketContainer${Type}.
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */
<@gen_warning filename="BucketContainer.java "/>

public class BucketContainer${Type}<O extends OB${Type}> extends AbstractBucketContainer${Type}<O, BucketObject${Type}>{
		public BucketContainer${Type}(Index < O > index, int pivots, OBStore<TupleBytes> storage, byte[] key) {
				super(index, pivots, storage, key);
		}

		public BucketContainer${Type}(Index < O > index, int pivots, OBStore<TupleBytes> storage, byte[] key, int secondaryIndexPivot) {
				super(index, pivots, storage, key, secondaryIndexPivot);
		
	}
	
	protected BucketObject${Type} instantiateBucketObject(){
    	return new BucketObject${Type}();
    }

	protected BucketObject${Type}[] instantiateArray(int size){
			return new BucketObject${Type}[size];							
	}
    
	<@gen_warning filename="BucketContainer.java "/>
}
</#list>

