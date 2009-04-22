package net.obsearch.storage.cuckoo;
/**
 * Bob Jenkins's 64-bit hash function.
 * Adapted to Java from http://burtleburtle.net/bob/c/lookup8.c
 * The current implementation attempts to be almost as faithful as possible
 * to the original code. Further java-specific optimizations are required.
 */
public final class Jenkins64 implements HashFunction {

	public long compute(byte[] k) {
		return computeAux(k, k.length, 1);
	}

	private long computeAux(byte k[], int length, long level) {
		long a, b, c;
		int len;
		int pos = 0;
		/* Set up the internal state */
		len = length;
		a = b = level; /* the previous hash value */
		c = 0x9e3779b97f4a7c13L; /* the golden ratio; an arbitrary value */

		/* ---------------------------------------- handle most of the key */
		while (len >= 24) {
			a += (k[pos + 0] + ((long) k[pos + 1] << 8)
					+ ((long) k[pos + 2] << 16) + ((long) k[pos + 3] << 24)
					+ ((long) k[pos + 4] << 32) + ((long) k[pos + 5] << 40)
					+ ((long) k[pos + 6] << 48) + ((long) k[pos + 7] << 56));
			b += (k[pos + 8] + ((long) k[pos + 9] << 8)
					+ ((long) k[pos + 10] << 16) + ((long) k[pos + 11] << 24)
					+ ((long) k[pos + 12] << 32) + ((long) k[pos + 13] << 40)
					+ ((long) k[pos + 14] << 48) + ((long) k[pos + 15] << 56));
			c += (k[pos + 16] + ((long) k[pos + 17] << 8)
					+ ((long) k[pos + 18] << 16) + ((long) k[pos + 19] << 24)
					+ ((long) k[pos + 20] << 32) + ((long) k[pos + 21] << 40)
					+ ((long) k[pos + 22] << 48) + ((long) k[pos + 23] << 56));

			// mix 64
			a -= b;
			a -= c;
			a ^= (c >> 43);
			b -= c;
			b -= a;
			b ^= (a << 9);
			c -= a;
			c -= b;
			c ^= (b >> 8);
			a -= b;
			a -= c;
			a ^= (c >> 38);
			b -= c;
			b -= a;
			b ^= (a << 23);
			c -= a;
			c -= b;
			c ^= (b >> 5);
			a -= b;
			a -= c;
			a ^= (c >> 35);
			b -= c;
			b -= a;
			b ^= (a << 49);
			c -= a;
			c -= b;
			c ^= (b >> 11);
			a -= b;
			a -= c;
			a ^= (c >> 12);
			b -= c;
			b -= a;
			b ^= (a << 18);
			c -= a;
			c -= b;
			c ^= (b >> 22);
			// mix 64

			pos += 24;
			len -= 24;
		}

		/* ------------------------------------- handle the last 23 bytes */
		c += length;
		switch (len) /* all the case statements fall through */
		{
		case 23:
			c += ((long) k[pos + 22] << 56);
		case 22:
			c += ((long) k[pos + 21] << 48);
		case 21:
			c += ((long) k[pos + 20] << 40);
		case 20:
			c += ((long) k[pos + 19] << 32);
		case 19:
			c += ((long) k[pos + 18] << 24);
		case 18:
			c += ((long) k[pos + 17] << 16);
		case 17:
			c += ((long) k[pos + 16] << 8);
			/* the first byte of c is reserved for the length */
		case 16:
			b += ((long) k[pos + 15] << 56);
		case 15:
			b += ((long) k[pos + 14] << 48);
		case 14:
			b += ((long) k[pos + 13] << 40);
		case 13:
			b += ((long) k[pos + 12] << 32);
		case 12:
			b += ((long) k[pos + 11] << 24);
		case 11:
			b += ((long) k[pos + 10] << 16);
		case 10:
			b += ((long) k[pos + 9] << 8);
		case 9:
			b += ((long) k[pos + 8]);
		case 8:
			a += ((long) k[pos + 7] << 56);
		case 7:
			a += ((long) k[pos + 6] << 48);
		case 6:
			a += ((long) k[pos + 5] << 40);
		case 5:
			a += ((long) k[pos + 4] << 32);
		case 4:
			a += ((long) k[pos + 3] << 24);
		case 3:
			a += ((long) k[pos + 2] << 16);
		case 2:
			a += ((long) k[pos + 1] << 8);
		case 1:
			a += ((long) k[pos + 0]);
			/* case 0: nothing left to add */
		}

		// mix 64
		a -= b;
		a -= c;
		a ^= (c >> 43);
		b -= c;
		b -= a;
		b ^= (a << 9);
		c -= a;
		c -= b;
		c ^= (b >> 8);
		a -= b;
		a -= c;
		a ^= (c >> 38);
		b -= c;
		b -= a;
		b ^= (a << 23);
		c -= a;
		c -= b;
		c ^= (b >> 5);
		a -= b;
		a -= c;
		a ^= (c >> 35);
		b -= c;
		b -= a;
		b ^= (a << 49);
		c -= a;
		c -= b;
		c ^= (b >> 11);
		a -= b;
		a -= c;
		a ^= (c >> 12);
		b -= c;
		b -= a;
		b ^= (a << 18);
		c -= a;
		c -= b;
		c ^= (b >> 22);
		// mix 64

		/* -------------------------------------------- report the result */
		return c;
	}

}
