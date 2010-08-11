package net.obsearch.index.sorter;

import net.obsearch.Storable;

/**
 * A sortable (from the point of view of a query) projection
 *
 */
public interface Projection<O, CP> extends Comparable<O>{
	
	
	CP getCompactRepresentation();
	
	
	byte[] getAddress();
	
	
	/**
	 * Calculate the distance of this projection and the given
	 * compact representation.
	 * @param b
	 * @return
	 */
	 O distance(CP b);

}
