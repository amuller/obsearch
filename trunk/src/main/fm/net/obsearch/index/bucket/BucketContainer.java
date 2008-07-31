<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="BucketContainer"+Type+".java" />
package net.obsearch.index.bucket;
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
	*  BucketContainer is a dummy class that is used to directly
  *  inherit the functionality from AbstractBucketContainer${Type}.
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */
import java.nio.ByteBuffer;

import net.obsearch.Index;
import net.obsearch.ob.OB${Type};

public class BucketContainer${Type}<O extends OB${Type}> extends AbstractBucketContainer${Type}<O, BucketObject${Type}>{

	public BucketContainer${Type}(Index<O> index, ByteBuffer data, int pivots) {
		super(index, data, pivots);
		
	}
	
	protected BucketObject${Type} instantiateBucketObject(){
    	return new BucketObject${Type}();
    }
    

}
</#list>

