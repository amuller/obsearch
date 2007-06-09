package org.ajmm.obsearch.index.pptree;

public class SpaceTreeNode implements SpaceTree {

	private int DD;

	private float DV;

	private SpaceTree left;

	private SpaceTree right;

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

}
