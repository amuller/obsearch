package net.obsearch.index.perm;

import java.util.Arrays;

/**
 * Compact distance permutation
 * @author Arnoldo Jose Muller Molina
 *
 */
public class CompactPerm {
	
	public short[] perm;

	public CompactPerm(short[] perm) {
		super();
		this.perm = perm;
	}

	@Override
	public boolean equals(Object obj) {
		CompactPerm p = (CompactPerm)obj;
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
