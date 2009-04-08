<#include "/@inc/ob.ftl">
<#-- default exists implementation -->
<#macro existsDefault>
  <@gen_simple_warning/>
	@Override
 /*
  * This is a basic exists function created on top of 
  * searchOB. It is a scaffold for different indexes. 
  */
	public OperationStatus exists(O object) throws OBException,
			IllegalAccessException, InstantiationException {
		OBPriorityQueue${Type}<O> result = new OBPriorityQueue${Type}<O>((byte) 1);
		searchOB(object, (${type}) 0, result);
		OperationStatus res = new OperationStatus();
		res.setStatus(Status.NOT_EXISTS);
		if (result.getSize() == 1) {
			Iterator<OBResult${Type}<O>> it = result.iterator();
			OBResult${Type}<O> r = it.next();
			if (r.getDistance() == 0) {
				res.setId(r.getId());
				res.setStatus(Status.EXISTS);
			}
		}
		return res;
	}
</#macro>

<#-- unsupported intersectingBoxes method -->
<#macro intersectingBoxesUnsupported>
	<@gen_simple_warning/>
	@Override
	public Iterator<Long> intersectingBoxes(O object, ${type} r)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}
</#macro>


<#-- unsupported intersects method -->
<#macro intersectsUnsupported>
<@gen_simple_warning/>
@Override
	public boolean intersects(O object, ${type} r, int box)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}
</#macro>

<#macro searchOBBoxesUnsupported>
		<@gen_simple_warning/>		
		@Override
	public void searchOB(O object, ${type} r, OBPriorityQueue${Type}<O> result,
			int[] boxes) throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		throw new UnsupportedOperationException();
	}
</#macro>