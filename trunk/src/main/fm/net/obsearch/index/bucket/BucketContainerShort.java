<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@binding_info t=t/>
<@pp.changeOutputFile name="BucketContainer"+Type+".java" />
package net.obsearch.index.bucket;

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

