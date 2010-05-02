package net.obsearch.index.permprefixhamming;

import java.util.Arrays;

/**
 * Compact distance permutation
 * @author Arnoldo Jose Muller Molina
 *
 */
public class CompactPermPrefix {
	
	public short[] perm;

	public CompactPermPrefix(short[] perm) {
		super();
		this.perm = perm;
	}

	@Override
	public boolean equals(Object obj) {
		CompactPermPrefix p = (CompactPermPrefix)obj;
		return Arrays.equals(perm, p.perm);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(perm);
	}
	
	public void set(int i, short pivot){
		perm[i] = pivot;
	}
	
	
}
