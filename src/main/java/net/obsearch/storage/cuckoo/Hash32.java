package net.obsearch.storage.cuckoo;

import java.util.Arrays;

/**
 * A collection of 32-bit hash tables that can be mixed by the classes below.
 * 
 * @author amuller
 * 
 */
public abstract class Hash32 implements HashFunction{

	private static long INT_MASK = 0x00000000ffffffffL;
	private static long BYTE_MASK = 0x00000000000000ffL;

	private final static long rot(long val, int pos) {
		return ((Integer.rotateLeft((int) (val & INT_MASK), pos)) & INT_MASK);
	}

	/**
	 * Java standard hash code.
	 * @param data
	 * @return
	 */
	protected int java(byte[] data) {
		return Arrays.hashCode(data);
	}
	
	 public int jenkins(byte[] data){
		 return jenkinsAux(data, data.length, 1);
	 }
	
	/**
	   * taken from  hashlittle() -- hash a variable-length key into a 32-bit value
	   * 
	   * @param key the key (the unaligned variable-length array of bytes)
	   * @param nbytes number of bytes to include in hash
	   * @param initval can be any integer value
	   * @return a 32-bit value.  Every bit of the key affects every bit of the
	   * return value.  Two keys differing by one or two bits will have totally
	   * different hash values.
	   * 
	   * <p>The best hash table sizes are powers of 2.  There is no need to do mod
	   * a prime (mod is sooo slow!).  If you need less than 32 bits, use a bitmask.
	   * For example, if you need only 10 bits, do
	   * <code>h = (h & hashmask(10));</code>
	   * In which case, the hash table should have hashsize(10) elements.
	   * 
	   * <p>If you are hashing n strings byte[][] k, do it like this:
	   * for (int i = 0, h = 0; i < n; ++i) h = hash( k[i], h);
	   * 
	   * <p>By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
	   * code any way you wish, private, educational, or commercial.  It's free.
	   * 
	   * <p>Use for hash table lookup, or anything where one collision in 2^^32 is
	   * acceptable.  Do NOT use for cryptographic purposes.
	  */	  
	  @SuppressWarnings("fallthrough")
	  public int jenkinsAux(byte[] key, int nbytes, int initval) {
	    int length = nbytes;
	    long a, b, c;       // We use longs because we don't have unsigned ints
	    a = b = c = (0x00000000deadbeefL + length + initval) & INT_MASK;
	    int offset = 0;
	    for (; length > 12; offset += 12, length -= 12) {
	      a = (a + (key[offset + 0]    & BYTE_MASK)) & INT_MASK;
	      a = (a + (((key[offset + 1]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
	      a = (a + (((key[offset + 2]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
	      a = (a + (((key[offset + 3]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
	      b = (b + (key[offset + 4]    & BYTE_MASK)) & INT_MASK;
	      b = (b + (((key[offset + 5]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
	      b = (b + (((key[offset + 6]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
	      b = (b + (((key[offset + 7]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
	      c = (c + (key[offset + 8]    & BYTE_MASK)) & INT_MASK;
	      c = (c + (((key[offset + 9]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
	      c = (c + (((key[offset + 10] & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
	      c = (c + (((key[offset + 11] & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
	      
	      /*
	       * mix -- mix 3 32-bit values reversibly.
	       * This is reversible, so any information in (a,b,c) before mix() is
	       * still in (a,b,c) after mix().
	       * 
	       * If four pairs of (a,b,c) inputs are run through mix(), or through
	       * mix() in reverse, there are at least 32 bits of the output that
	       * are sometimes the same for one pair and different for another pair.
	       * 
	       * This was tested for:
	       * - pairs that differed by one bit, by two bits, in any combination
	       *   of top bits of (a,b,c), or in any combination of bottom bits of
	       *   (a,b,c).
	       * - "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
	       *   the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
	       *    is commonly produced by subtraction) look like a single 1-bit
	       *    difference.
	       * - the base values were pseudorandom, all zero but one bit set, or
	       *   all zero plus a counter that starts at zero.
	       * 
	       * Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
	       * satisfy this are
	       *     4  6  8 16 19  4
	       *     9 15  3 18 27 15
	       *    14  9  3  7 17  3
	       * Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing for 
	       * "differ" defined as + with a one-bit base and a two-bit delta.  I
	       * used http://burtleburtle.net/bob/hash/avalanche.html to choose
	       * the operations, constants, and arrangements of the variables.
	       * 
	       * This does not achieve avalanche.  There are input bits of (a,b,c)
	       * that fail to affect some output bits of (a,b,c), especially of a.
	       * The most thoroughly mixed value is c, but it doesn't really even
	       * achieve avalanche in c.
	       * 
	       * This allows some parallelism.  Read-after-writes are good at doubling
	       * the number of bits affected, so the goal of mixing pulls in the
	       * opposite direction as the goal of parallelism.  I did what I could.
	       * Rotates seem to cost as much as shifts on every machine I could lay
	       * my hands on, and rotates are much kinder to the top and bottom bits,
	       * so I used rotates.
	       *
	       * #define mix(a,b,c) \
	       * { \
	       *   a -= c;  a ^= rot(c, 4);  c += b; \
	       *   b -= a;  b ^= rot(a, 6);  a += c; \
	       *   c -= b;  c ^= rot(b, 8);  b += a; \
	       *   a -= c;  a ^= rot(c,16);  c += b; \
	       *   b -= a;  b ^= rot(a,19);  a += c; \
	       *   c -= b;  c ^= rot(b, 4);  b += a; \
	       * }
	       * 
	       * mix(a,b,c);
	       */
	      a = (a - c) & INT_MASK;  a ^= rot(c, 4);  c = (c + b) & INT_MASK;
	      b = (b - a) & INT_MASK;  b ^= rot(a, 6);  a = (a + c) & INT_MASK;
	      c = (c - b) & INT_MASK;  c ^= rot(b, 8);  b = (b + a) & INT_MASK;
	      a = (a - c) & INT_MASK;  a ^= rot(c,16);  c = (c + b) & INT_MASK;
	      b = (b - a) & INT_MASK;  b ^= rot(a,19);  a = (a + c) & INT_MASK;
	      c = (c - b) & INT_MASK;  c ^= rot(b, 4);  b = (b + a) & INT_MASK;
	    }

	    //-------------------------------- last block: affect all 32 bits of (c)
	    switch (length) {                   // all the case statements fall through
	    case 12:
	      c = (c + (((key[offset + 11] & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
	    case 11:
	      c = (c + (((key[offset + 10] & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
	    case 10:
	      c = (c + (((key[offset + 9]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
	    case  9:
	      c = (c + (key[offset + 8]    & BYTE_MASK)) & INT_MASK;
	    case  8:
	      b = (b + (((key[offset + 7]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
	    case  7:
	      b = (b + (((key[offset + 6]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
	    case  6:
	      b = (b + (((key[offset + 5]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
	    case  5:
	      b = (b + (key[offset + 4]    & BYTE_MASK)) & INT_MASK;
	    case  4:
	      a = (a + (((key[offset + 3]  & BYTE_MASK) << 24) & INT_MASK)) & INT_MASK;
	    case  3:
	      a = (a + (((key[offset + 2]  & BYTE_MASK) << 16) & INT_MASK)) & INT_MASK;
	    case  2:
	      a = (a + (((key[offset + 1]  & BYTE_MASK) <<  8) & INT_MASK)) & INT_MASK;
	    case  1:
	      a = (a + (key[offset + 0]    & BYTE_MASK)) & INT_MASK;
	      break;
	    case  0:
	      return (int)(c & INT_MASK);
	    }
	    /*
	     * final -- final mixing of 3 32-bit values (a,b,c) into c
	     * 
	     * Pairs of (a,b,c) values differing in only a few bits will usually
	     * produce values of c that look totally different.  This was tested for
	     * - pairs that differed by one bit, by two bits, in any combination
	     *   of top bits of (a,b,c), or in any combination of bottom bits of
	     *   (a,b,c).
	     * 
	     * - "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
	     *   the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
	     *   is commonly produced by subtraction) look like a single 1-bit
	     *   difference.
	     * 
	     * - the base values were pseudorandom, all zero but one bit set, or
	     *   all zero plus a counter that starts at zero.
	     * 
	     * These constants passed:
	     *   14 11 25 16 4 14 24
	     *   12 14 25 16 4 14 24
	     * and these came close:
	     *    4  8 15 26 3 22 24
	     *   10  8 15 26 3 22 24
	     *   11  8 15 26 3 22 24
	     * 
	     * #define final(a,b,c) \
	     * { 
	     *   c ^= b; c -= rot(b,14); \
	     *   a ^= c; a -= rot(c,11); \
	     *   b ^= a; b -= rot(a,25); \
	     *   c ^= b; c -= rot(b,16); \
	     *   a ^= c; a -= rot(c,4);  \
	     *   b ^= a; b -= rot(a,14); \
	     *   c ^= b; c -= rot(b,24); \
	     * }
	     * 
	     */
	    c ^= b; c = (c - rot(b,14)) & INT_MASK;
	    a ^= c; a = (a - rot(c,11)) & INT_MASK;
	    b ^= a; b = (b - rot(a,25)) & INT_MASK;
	    c ^= b; c = (c - rot(b,16)) & INT_MASK;
	    a ^= c; a = (a - rot(c,4))  & INT_MASK;
	    b ^= a; b = (b - rot(a,14)) & INT_MASK;
	    c ^= b; c = (c - rot(b,24)) & INT_MASK;

	    return (int)(c & INT_MASK);
	  }


	/**
	 * One at a time hash
	 * 
	 * @param key
	 * @return
	 */
	protected int joaat(byte[] key) {
		int hash = 0;

		for (byte b : key) {
			hash += (b & 0xFF);
			hash += (hash << 10);
			hash ^= (hash >>> 6);
		}
		hash += (hash << 3);
		hash ^= (hash >>> 11);
		hash += (hash << 15);
		return hash;
	}
	
	/**
	 * One at a time hash
	 * 
	 * @param key
	 * @return
	 */
	public static int joaatAux(byte[] key, int start, int end) {
		int hash = 0;
		int i = start;
		while(i < end){ 
			byte b = key[i];
			hash += (b & 0xFF);
			hash += (hash << 10);
			hash ^= (hash >>> 6);
		}
		hash += (hash << 3);
		hash ^= (hash >>> 11);
		hash += (hash << 15);
		return hash;
	}

	public  int murmur(byte[] data) {
		return murmurAux(data, 1);
	}

	/**
	 * Apache license ( written by Andrzej Bialecki) TODO: update license thing.
	 * Taken from: http://www.getopt.org/murmur/MurmurHash.java
	 * 
	 * @param data
	 * @param seed
	 * @return
	 */
	public  int murmurAux(byte[] data, int seed) {
		int m = 0x5bd1e995;
		int r = 24;

		int h = seed ^ data.length;

		int len = data.length;
		int len_4 = len >> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		int len_m = len_4 << 2;
		int left = len - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= (int) data[len - 3] << 16;
			}
			if (left >= 2) {
				h ^= (int) data[len - 2] << 8;
			}
			if (left >= 1) {
				h ^= (int) data[len - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;
	}

}
