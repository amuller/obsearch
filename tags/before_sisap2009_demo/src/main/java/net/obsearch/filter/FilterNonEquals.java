package net.obsearch.filter;
import net.obsearch.OB;


/**
 * Filter all objects that are not equal to the query.
 * Useful sometimes.
 *
 */
public class FilterNonEquals<O extends OB> implements Filter<O> {

	@Override
	public boolean accept(O dbObject, O query) {		
		return ! dbObject.equals(query);
	}

}
