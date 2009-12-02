package net.obsearch.index.ky0;

import hep.aida.bin.StaticBin1D;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.index.perm.impl.PerDouble;
import net.obsearch.index.sorter.Projection;
import net.obsearch.utils.bytes.ByteConversion;

public class Ky0Projection implements Projection<Ky0Projection, CompactKy0> {

	private CompactKy0 addr;
	private int heightDistance;
	private int distance;

	public Ky0Projection(CompactKy0 addr, int heightDistance, int distance) {
		this.addr = addr;
		this.distance = distance;
		this.heightDistance = heightDistance;
	}
	
	public int getHeight(){
		return heightDistance;
	}

	public Ky0Projection(double[] pivotDist) throws OBException {
		OBAsserts.chkAssert(pivotDist.length <= Short.MAX_VALUE,
				"max pivots exeeded");
		List<PerDouble> p = new ArrayList<PerDouble>(pivotDist.length);
		short i = 0;
		for (double d : pivotDist) {
			p.add(new PerDouble(d, i));
			i++;
		}
		Collections.sort(p);
		// populate permutations

		int[] permutations = new int[pivotDist.length];

		i = 0;
		for (PerDouble d : p) {
			permutations[i] = d.getId();
			i++;
		}

		int[] inversionTableRight = new int[permutations.length];
		int height = 0;
		// populate inversion table
		i = 0;
		for (int d : permutations) {
			int k = countBiggerToRight(d, permutations, i);
			inversionTableRight[i] = k;
			height += k;
			i++;
		}

		addr = new CompactKy0(height, inversionTableRight);
		distance = -1;
	}

	private int countBiggerToRight(int perm, int[] perms, int index) {
		int i = index;
		int res = 0;
		while (i < perms.length) {
			if (perms[i] < perm) {
				res++;
			}
			i++;
		}
		return res;
	}

	public int getDistance() {
		return distance;
	}

	@Override
	public byte[] getAddress() {
		return addr.getAddress();
	}

	@Override
	public CompactKy0 getCompactRepresentation() {
		return addr;
	}

	@Override
	public int compareTo(Ky0Projection o) {
		
		if (this.heightDistance < o.heightDistance) {
			return -1;
		} else if (this.heightDistance > o.heightDistance) {
			return 1;
		} else {
			if (distance < o.distance) {
				return -1;
			} else if (distance > o.distance) {
				return 1;
			} else {
				return 0;
			}
		}
		
		
	}

	@Override
	public Ky0Projection distance(CompactKy0 b) {
		int res = 0;
		int i = 0;
		int mode = -2;
		int height = Math.abs(addr.getHeight() - b.getHeight());
		if (addr.getHeight() == b.getHeight()) {
			mode = 0;
		} else if (addr.getHeight() < b.getHeight()) {
			mode = -1;
		} else {
			mode = 1;
		}
		while (i < b.getValue().length) {
			int a1 = addr.getInvertedPermutation()[i];
			int a2 = b.getInvertedPermutation()[i];
			res += Math.abs(a1 - a2);
			/*if (mode == -1 && a1 <= a2) {
				res += a2 - a1;
			} else if (mode == 1 && a1 >= a2) {
				res += a1 - a2;
			} else if (mode == 0 && a1 == a2) {
				// nothing to do
			} else {
				// not full containment, send the score to inf!
				res = Integer.MAX_VALUE;
				//kHeight = Integer.MAX_VALUE;
				break;
			}*/
			i++;
		}

		return new Ky0Projection(b, height, res);
	}

	public String toString() {
		return "Found at dist: " + distance;
	}

}
