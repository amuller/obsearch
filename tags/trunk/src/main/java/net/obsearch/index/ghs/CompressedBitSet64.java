package net.obsearch.index.ghs;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.result.OBResultInvertedByte;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2009 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * CompressedBitSet64 stores bits in a byte array. The bit set must be first
 * created (stored in a temporary file) And then the bytes will be loaded into
 * memory. The compressed bit set works on longs and it allows sequential k-nn
 * searches of longs with the hamming distance. Insertions must be done in
 * ascending order. The main assumption is that compression will allow small
 * indexes that will be stored in memory.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public class CompressedBitSet64 {

	protected byte[] data;
	private File f;
	private OutputBitStream out;
	protected int count = 0;
	protected long first;
	private long previous;
	private boolean commit;

	/**
	 * Create a new compressed bit set.
	 * 
	 * @throws IOException
	 */
	public CompressedBitSet64() throws OBException {
		try {
			f = File.createTempFile("OBSearch", "bitset");
			FileOutputStream o = new FileOutputStream(f);
			out = new OutputBitStream(o);
		} catch (IOException e) {
			throw new OBException(e);
		}
		commit = false;
	}

	/**
	 * Add the ith bit to this bitset.
	 * 
	 * @param bit
	 * @throws OBException
	 */
	public void add(long bit) throws OBException {
		OBAsserts.chkAssert(!commit, "Cannot add when commited");
		if (count == 0) {
			first = bit;
		} else {
			OBAsserts.chkAssert(previous < bit,
					"Elements must be ordered from lower to higher values");
			long d = bit - previous;
			try {
				write(d, out);
			} catch (IOException e) {
				throw new OBException(e);
			}
		}
		previous = bit;
		count++;
	}

	/**
	 * We will stop adding values and now we will use the bit set for search.
	 * 
	 * @throws OBException
	 */
	public void commit() throws OBException {
		OBAsserts.chkAssert(!commit, "Cannot commit when commited");
		try {
			out.close();
			long byteSize = f.length();
			OBAsserts.chkAssert(byteSize <= Integer.MAX_VALUE,
					"Exceeded allowed index size");
			data = new byte[(int) byteSize];
			FileInputStream fi = new FileInputStream(f);
			fi.read(data);
			fi.close();
			f.delete();
		} catch (IOException e) {
			throw new OBException(e);
		}
	}

	public long getBytesSize() {
		return data.length;
	}

	/**
	 * Hamming distance used for searching.
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public final int bucketDistance(long a, long b) {
		return Long.bitCount(a ^ b);
	}

	/**
	 * Search the maxF closest buckets by hamming distance for the given query
	 * 
	 * @param query
	 *            The query we will use
	 * @param maxF
	 *            The number of objects that will be searched.
	 * @param m
	 *            the number of bits == max distance expected.
	 * @return The closest objects to the given query.
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws OBException
	 */
	public long[] searchBuckets(long query, int maxF, int m)
			throws OBException {

		InputBitStream delta = new InputBitStream(data);
		long[] result = new long[maxF];
		FastPriorityQueueLong l = new FastPriorityQueueLong(m, maxF);

		// do the first element.
		int distance = bucketDistance(query, first);
		// previous bucket id.
		long prev = first;
		l.add(first, distance);
		// do the rest
		try {
			int i = 1;
			while (i < count) {
				long object = read(delta) + prev;
				distance = bucketDistance(query, object);
				l.add(object, distance);
				prev = object;
				i++;
			}
		} catch (IOException e) {
			throw new OBException(e);
		}
		return l.get();
	}
	
	/**
	 * Search the entire bitset. This should be done with datasets
	 * that can fit in memory and only to generate statistics
	 * on the data.
	 * @param query query to search
	 * @return
	 */
	public List<OBResultInvertedByte<Long>> searchFull(long query) throws OBException {
		List<OBResultInvertedByte<Long>> result = new ArrayList<OBResultInvertedByte<Long>>(size());
		InputBitStream delta = new InputBitStream(data);
		// do the first element.
		int distance = bucketDistance(query, first);
		result.add(new OBResultInvertedByte<Long>(first, first, (byte)distance));
		long prev = first;
		int i = 1;
		try {
		while (i < count) {
			long object;
			
				object = read(delta) + prev;
			
			distance = bucketDistance(query, object);
			assert distance <= Byte.MAX_VALUE;
			result.add(new OBResultInvertedByte<Long>(object, object, (byte)distance));
			prev = object;
			i++;
		}
		Collections.sort(result);
		return result;
		} catch (IOException e) {
			throw new OBException(e);
		}
	}
	
	public int size(){
		return count;
	}
	
	/**
	 * Return all the buckets just for debugging purposes.
	 * @return
	 * @throws IOException 
	 */
	protected long[] getAll() throws IOException{
		long[] result = new long[count];
		long prev = first;
		int i = 0;
		result[i] = prev;
		i++;
		InputBitStream delta = new InputBitStream(data);
		while(i < result.length){
			result[i] = read(delta) + prev;
			prev = result[i];
			i++;
		}
		return result;
	}
	
	private void write(long object, OutputBitStream out) throws IOException{
		out.writeLongDelta(object);
	}
	
	protected long read(InputBitStream in) throws IOException{
		return in.readLongDelta();
	}

}
