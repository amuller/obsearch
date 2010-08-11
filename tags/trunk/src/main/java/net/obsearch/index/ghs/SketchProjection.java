package net.obsearch.index.ghs;

import java.io.IOException;
import java.util.BitSet;

import cern.colt.bitvector.BitVector;

import net.obsearch.exception.OBException;
import net.obsearch.index.sorter.Projection;

/**
 *   
 */
public final class SketchProjection implements Projection<SketchProjection, CBitVector> {

	/**
	 * ordered pivot pair positions from greater to smaller.
	 */

	//private int magnitude;
	//private byte[] ordering;
	private int distance;
	//private double[] lowerBounds;
	//private double lowerBound;
	private CBitVector sketch;
	
	public String toString(){
		return "[" + distance + "]";
		
	}

	public SketchProjection(byte[] ordering, CBitVector sketch, int distance, double[] lowerBounds) {
		// this.ordering = ordering;
		/*magnitude = 0;
		for (byte b : ordering) {
			magnitude += b;
		}
		lowerBound = 0;
		for(double d : lowerBounds){
			lowerBound += d;
		}
		
		this.ordering = ordering;
		
		this.lowerBounds = lowerBounds;
		*/
		this.sketch = sketch;
		this.distance = distance;
	}
	
	
	
	/*public double[] getLowerBounds(){
		return lowerBounds;
	}

	public byte[] getOrdering() {
		return ordering;
	}*/
	
	public void setDistance(int distance) {
		this.distance = distance;
	}

	public void setSketch(CBitVector sketch) {
		this.sketch = sketch;
	}

	public int getDistance(){
		return distance;
	}
	
	public boolean equals(Object o){
		SketchProjection p = (SketchProjection) o;
		return p.sketch.equals(sketch);
	}

	@Override
	/*
	 * public int compareTo(SketchProjection o) { int cx0 = 0; int cx1 = 0; assert
	 * ordering.length == o.ordering.length; while(cx0 < ordering.length && cx1
	 * < ordering.length){ if(ordering[cx0] > o.ordering[cx1]){ cx0++; }else
	 * if(ordering[cx0] < o.ordering[cx1]){ cx1++; }else{ cx0++; cx1++; } }
	 * if(cx0 > cx1){ return -1; }else if(cx0 < cx1){ return 1; }else{ return 0;
	 * } }
	 */
	public int compareTo(SketchProjection o) {
		if (distance < o.distance) {
			return -1;
		} else if (distance > o.distance) {
			return 1;
		}else{
			return 0;
		}/*
		else { // proceed with magnitudes.
			if (lowerBound > o.lowerBound) {
				return -1;
			} else if (lowerBound < o.lowerBound) {
				return 1;
			} else {
				return 0;
			}
		}*/

	}
	
	public CBitVector getCompactRepresentation(){
		return sketch;
	}

	public CBitVector getSketch() {
		return sketch;
	}

	
	public byte[] getAddress(){
		return sketch.store();
	}
	
	/**
	 * Calculate the distance between the given bitvector and this sketch projection
	 * Return a sketch projection with the quality measurements based on the data found in this object.
	 * @param b
	 * @return
	 */
	public SketchProjection distance(CBitVector b){
		
		assert b.size() == sketch.size();
/*		CBitVector hamming = b.copy();
		hamming.xor(sketch);
		
		int distance = hamming.cardinality();*/
		
		int distance = b.hamming(sketch);
		/*int i = 0;
		byte[] ordering = new byte[distance];
		double[] lowerBounds = new double[b.size() - distance];
		int cx = 0;
		while (i < b.size() && cx < distance) {
			if (! hamming.get(i)) {
				// ith bit is set
				ordering[cx] = getOrdering()[i];
				lowerBounds[cx] = getLowerBounds()[i];
				cx++;
			}
			i++;
		}*/
		// return new SketchProjection(ordering, b, distance, lowerBounds);
		
		return new SketchProjection(null, b, distance, null);
		
	}
	
	public int hamming(CBitVector b){
		return b.hamming(sketch);
	}

}
