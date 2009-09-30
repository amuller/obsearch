package net.obsearch.index.ghs;

import java.util.ArrayList;
import java.util.Iterator;

import cern.colt.bitvector.BitVector;

/**
 * This data structure simulates a priority queue of objects sorted by their
 * distance to some other object. It assumes several things for performance
 * reasons: 1)Distances are discrete. 2) Objects to be stored are longs 3) The
 * maximum distance value is a value m that is very small (64 for example :)).
 * This priority queue uses a lot of memory if queueSize is too large.
 * 
 * @author Arnoldo Jose Muller Molina
 * 
 */
public final class SketchPriorityQueue implements Iterable<SketchProjection>{

	private FixedPriorityQueue<SketchProjection>[] levels;
	private final int queueSize;
	private int currentMaxDistance = -1;

	/**
	 * Creates the queue
	 * 
	 * @param maxDistance
	 *            maximum distance that will be inserted in the queue. (from 0
	 *            to maxDistance)
	 * @param queueSize
	 *            size of the queue (number of closest elements to be obtained).
	 */
	public SketchPriorityQueue(int maxDistance, int queueSize) {
		maxDistance++;
		int i = 0;
		levels = new FixedPriorityQueue[maxDistance];
		while (i < maxDistance) {
			levels[i] = new FixedPriorityQueue<SketchProjection>(queueSize);
			i++;
		}
		this.queueSize = queueSize;
	}

	/**
	 * 
	 * @return The max distance 
	 */
	public int currentMaxDistance() {
		updateCurrentMaxDistance();
		return currentMaxDistance;
	}

	private void updateCurrentMaxDistance() {
		if (currentMaxDistance == -1) {
			int i = 0;
			int count = 0;
			while (i < levels.length) {
				count += levels[i].size();
				if (count >= queueSize) {
					break;
				}
				i++;
			}
			currentMaxDistance = i;
		}
	}

	/**
	 * Add an object that is at a certain distance.
	 * 
	 * @param object
	 *            the object to add
	 * @param distance
	 *            the distance of the object.
	 */
	public void add(SketchProjection object) {
		levels[object.getDistance()].add(object);
		currentMaxDistance = -1;
	}

	/**
	 * Return the closeXBst long objects We do not include the original distances
	 * to reduce the creation of objects.
	 * 
	 * @return the closest long objects
	 */
	public CBitVector[] get() {
		ArrayList<SketchProjection> r = getResults();
		CBitVector[] result = new CBitVector[r.size()];
		int cx = 0;
		for(SketchProjection s : r){
			result[cx] = s.getSketch();
		}
		return result;
	}
	
	public ArrayList<SketchProjection> getResults(){
		int total = 0;
		for (FixedPriorityQueue<SketchProjection> c : levels) {
			total += c.size();
		}
		total = Math.min(total, queueSize);
		ArrayList<SketchProjection> result = new ArrayList<SketchProjection>(total);
		int i = 0;
		while(i < levels.length && result.size() <= total){
			for(SketchProjection s : levels[i]){
				result.add(s);
			}
			i++;
		}
		return result;
	}

	@Override
	public Iterator<SketchProjection> iterator() {
		

		return getResults().iterator();
	}

}
