package org.ajmm.obsearch.index.pptree;

public class SpaceTreeLeaf implements SpaceTree {

	int SNo;

	float [] a;

	float[] b;

	float [] c;

	public float[] getA() {
		return a;
	}
	public void setA(float[] a) {
		this.a = a;
	}
	public float[] getB() {
		return b;
	}
	public void setB(float[] b) {
		this.b = b;
	}
	public float[] getC() {
		return c;
	}
	public void setC(float[] c) {
		this.c = c;
	}
	public int getSNo() {
		return SNo;
	}
	public void setSNo(int no) {
		SNo = no;
	}

	public boolean isLeafNode() {
		return true;
	}

}
