<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="RF04PivotSelector${Type}.java" />

package net.obsearch.pivots.rf04.impl;

import cern.colt.list.LongArrayList;
import net.obsearch.Index;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;
import net.obsearch.ob.OB${Type};
import net.obsearch.pivots.rf04.AbstractIncrementalRF04;

public class RF04PivotSelector${Type}<O extends OB${Type}> extends AbstractIncrementalRF04<O> {

	public RF04PivotSelector${Type}(Pivotable<O> pivotable) {
		super(pivotable);		
	}

	@Override
	protected double distance(O a, O b) throws OBException {
		return a.distance(b);
	}

	@Override
	public PivotResult generatePivots(int pivotCount, LongArrayList elements,
			Index<O> index) throws OBException, IllegalAccessException,
			InstantiationException, OBStorageException,
			PivotsUnavailableException {
		
		return null;
	}
	
	

}

</#list>