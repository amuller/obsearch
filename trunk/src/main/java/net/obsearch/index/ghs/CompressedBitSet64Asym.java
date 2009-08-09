package net.obsearch.index.ghs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import it.unimi.dsi.io.InputBitStream;
import net.obsearch.exception.OBException;

public class CompressedBitSet64Asym extends CompressedBitSet64 {

	public CompressedBitSet64Asym() throws OBException {
		super();

	}

	public long[] searchBuckets(SketchProjection query, int maxF, int m)
			throws OBException {

		InputBitStream delta = new InputBitStream(data);
		long[] result = new long[maxF];
		SketchPriorityQueue l = new SketchPriorityQueue(m, maxF);

		// do the first element.

		// previous bucket id.
		long prev = first;
		addToQueue(query, first, l, m);

		// do the rest
		try {
			int i = 1;
			while (i < count) {
				long object = read(delta) + prev;
				addToQueue(query, object, l, m);
				prev = object;
				i++;
			}
		} catch (IOException e) {
			throw new OBException(e);
		}
		return l.get();
	}

	public List<SketchProjection> searchFullAsym(SketchProjection query, int m)
			throws OBException {

		ArrayList<SketchProjection> result = new ArrayList<SketchProjection>(size());

		InputBitStream delta = new InputBitStream(data);
		// do the first element.

		result.add(createSketchResult(query, first, m));

		long prev = first;
		int i = 1;
		try {
			while (i < count) {
				long object;

				object = read(delta) + prev;

				result.add(createSketchResult(query, object, m));
				prev = object;
				i++;
			}
			Collections.sort(result);
			return result;
		} catch (IOException e) {
			throw new OBException(e);
		}
	}

	private SketchProjection createSketchResult(SketchProjection query, long obj,
			int m) {
		long hamming = query.getSketch() ^ obj;
		int distance = Long.bitCount(hamming);
		assert distance <= Byte.MAX_VALUE;
		int i = 0;
		byte[] ordering = new byte[distance];
		double[] lowerBounds = new double[distance];
		int cx = 0;
		while (i < m && cx < distance) {
			if ((hamming & (1 << i)) != 0) {
				// ith bit is set
				ordering[cx] = query.getOrdering()[i];
				lowerBounds[cx] = query.getLowerBounds()[i];
				cx++;
			}
			i++;
		}
		return new SketchProjection(ordering, obj, distance, lowerBounds);
	}

	private void addToQueue(SketchProjection query, long obj,
			SketchPriorityQueue l, int m) {

		SketchProjection toAdd = createSketchResult(query, obj, m);
		if (toAdd.getDistance() > l.currentMaxDistance()) {
			return;
		}
		l.add(toAdd);
	}

}
