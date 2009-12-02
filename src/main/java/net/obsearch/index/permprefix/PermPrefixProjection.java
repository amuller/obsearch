package net.obsearch.index.permprefix;

import hep.aida.bin.StaticBin1D;

import java.nio.ByteBuffer;

import net.obsearch.constants.ByteConstants;
import net.obsearch.index.sorter.Projection;
import net.obsearch.utils.bytes.ByteConversion;

public class PermPrefixProjection implements
		Projection<PermPrefixProjection, CompactPermPrefix> {

	private static final int subPrefix = 256;

	private CompactPermPrefix addr;
	private int distance;
	private int[] cache;
	private int maxMovement;
	private int[] newDistance;

	public PermPrefixProjection(CompactPermPrefix addr, int distance,
			int[] cache) {
		this(addr, distance, cache, -1, null);
	}

	public PermPrefixProjection(CompactPermPrefix addr, int distance,
			int[] cache, int maxMovement, int[] newDistance) {
		this.addr = addr;
		this.distance = distance;
		this.cache = cache;
		this.maxMovement = maxMovement;
		this.newDistance = newDistance;
	}

	public int getDistance() {
		return distance;
	}

	public int[] getDistances() {
		return newDistance;
	}

	@Override
	public byte[] getAddress() {
		return shortToBytes(addr.perm);
	}

	public static byte[] shortToBytes(short[] addr) {
		ByteBuffer res = ByteConversion.createByteBuffer(addr.length
				* ByteConstants.Short.getSize());
		for (short s : addr) {
			res.putShort(s);
		}
		return res.array();
	}

	@Override
	public CompactPermPrefix getCompactRepresentation() {
		return addr;
	}

	@Override
	public int compareTo(PermPrefixProjection o) {

		if (distance < o.distance) {
			return -1;
		} else if (distance > o.distance) {
			return 1;
		} else {
			return 0;
		}

		/*
		 * =======
		 * 
		 * >>>>>>> .r579 <<<<<<< .mine ======= if (distance < o.distance) {
		 * return -1; } else if (distance > o.distance) { return 1; } else {
		 * return 0; }
		 * 
		 * 
		 * >>>>>>> .r579 / int i = 0; while (i < o.newDistance.length) { int
		 * dist = newDistance[i]; int dist2 = o.newDistance[i]; if (dist <
		 * dist2) { return -1; } else if (dist > dist2) { return 1; } i++;
		 * 
		 * }
		 * 
		 * return 0;
		 */
	}

	@Override
	public PermPrefixProjection distance(CompactPermPrefix b) {
		int res = 0;
		int cx = 0;
		// int max = 0;
		StaticBin1D max = new StaticBin1D();
		// normal
		newDistance = new int[b.perm.length];
		while (cx < b.perm.length) {
			int obj = b.perm[cx];
			int herePos = cache[obj];
			// int herePos = Math.min(cache[obj], subPrefix); // not
			// int diff = Math.min(Math.abs( herePos - cx), b.perm.length); //
			// not marionette
			int diff = Math.abs(herePos - cx);// marionette
			newDistance[cx] = diff;
			max.add(diff);
			res += diff;
			cx++;
		}

		// hamming

		/*
		 * while (cx < b.perm.length) { int obj = b.perm[cx]; int herePos =
		 * cache[obj]; if (herePos >= b.perm.length) { res++; } cx++; }
		 */

		return new PermPrefixProjection(b, res, cache, (int) max.mean(),
				newDistance);
	}

	public int getMaxDiff() {
		return maxMovement;
	}

	public String toString() {
		return "Found at dist: " + distance;
	}

	public void set(int i, short pivot) {
		addr.set(i, pivot);
	}

}
