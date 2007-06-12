package org.ajmm.obsearch.index.pptree;

import java.util.List;

public class SpaceTreeNode implements SpaceTree {

	private int DD;

	private float DV;

	private SpaceTree left;

	private SpaceTree right;

	public String toString(){
		return "[DD: " + DD + " DV " + DV + "](" + left + "," + right +")";
	}

	public boolean isLeafNode() {
		return false;
	}

	public int getDD() {
		return DD;
	}

	public void setDD(int dd) {
		DD = dd;
	}

	public float getDV() {
		return DV;
	}

	public void setDV(float dv) {
		DV = dv;
	}

	public SpaceTree getLeft() {
		return left;
	}

	public void setLeft(SpaceTree left) {
		this.left = left;
	}

	public SpaceTree getRight() {
		return right;
	}

	public void setRight(SpaceTree right) {
		this.right = right;
	}

	public  SpaceTreeLeaf search(float[] value){
		if(value[DD] < DV){
			return left.search(value);
		}
		if(value[DD] >= DV){
			return right.search(value);
		}else{
			assert false;
			return null;
		}
	}

	/**
	 * Takes a query rectangle and returns all the spaces that
	 * intersect with it.
	 * @param query the query to be searched
	 * @param result will hold all the spaces that intersect with the query
	 */
	public void searchRange(float[][] query, List<SpaceTreeLeaf> result){
		// if the maximum value of the query in the given dimension is lower
		// than the split value, then we can safely ignore the other branches
		/*if(query[DD][MAX] < DV){
			left.searchRange(query, result);
		}else if (query[DD][MIN] >= DV){
			right.searchRange(query, result);
		}else{
			// our query has values that belong to both branches...
			// nothing we can do here, we have to explore everything.
			left.searchRange(query, result);
			right.searchRange(query, result);
		}*/
		if(query[DD][MIN] <= DV){
			left.searchRange(query, result);
		}

		if(query[DD][MAX] >= DV){
			right.searchRange(query, result);
		}
	}



}
