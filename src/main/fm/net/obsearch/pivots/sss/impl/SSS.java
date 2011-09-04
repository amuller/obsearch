<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="SSS${Type}.java" />

package net.obsearch.pivots.sss.impl;

import net.obsearch.exception.OBException;
import net.obsearch.ob.OB${Type};
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.Pivotable;
import net.obsearch.pivots.sss.AbstractMySSS;

public class SSS${Type}<O extends OB${Type}> extends AbstractMySSS<O>implements IncrementalPivotSelector<O>  {

		public SSS${Type}(Pivotable<O> pivotable) {
		super(pivotable);
	}

	@Override
	protected double distance(O o1, O o2) throws OBException {
		return o1.distance(o2);
	}

	

}


</#list>