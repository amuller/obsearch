package net.obsearch.index.ghs;

/**
 * This data structure simulates a priority queue of objects sorted by their
 * distance to some other object.
 * It assumes several things for performance reasons:
 * 1)Distances are discrete. 
 * 2) Objects to be stored are longs
 * 3) The maximum distance value is a value m that is very small (64 for example :)).
 * This priority queue uses a lot of memory if queueSize is too large. 
 * 
 * @author Arnoldo Jose Muller Molina
 *
 */
public final class FastPriorityQueueLong {
	
	private long[][] data;
	private int[] counts;
	private final int queueSize;
	/**
	 * Creates the queue
	 * @param maxDistance maximum distance that will be inserted in the queue. (from 0 to maxDistance)
	 * @param queueSize size of the queue (number of closest elements to be obtained).
	 */
	public FastPriorityQueueLong(int maxDistance, int queueSize){
		maxDistance++;
		data = new long[maxDistance][queueSize];
		counts = new int[maxDistance];
		this.queueSize = queueSize;
	}
	
	/**
	 * Add an object that is at a certain distance.
	 * @param object the object to add
	 * @param distance the distance of the object.
	 */
	public void add(long object, int distance){
		int index = counts[distance];
		if(index < queueSize){
			data[distance][index] = object;
			counts[distance]++;
		}
	}
	
	/**
	 * Return the closest long objects
	 * We do not include the original distances to reduce
	 * the creation of objects.
	 * @return the closest long objects
	 */
	public long[] get(){
		int total = 0;
		for(int c : counts){
			total += c;
		}
		total = Math.min(total, queueSize);
		long[] result = new long[total];
		int i1 = 0;
		
		int resi = 0;
		while(i1 < data.length && resi < total){
			int i2 = 0;
			while(i2 < counts[i1] && resi < total){
				result[resi] = data[i1][i2];
				resi++;
				i2++;
			}
			i1++;
		}
		return result;		
	}

}
