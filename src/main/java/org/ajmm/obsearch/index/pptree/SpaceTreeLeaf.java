package org.ajmm.obsearch.index.pptree;

import java.util.Arrays;
import java.util.List;

public class SpaceTreeLeaf implements SpaceTree {

	int SNo;

	double[] min;

	double[] width;

	double[] exp;

	public String toString(){
		return "leaf(" + SNo + " " + Arrays.deepToString(minMax)+ ")";
	}

	/**
	 * This holds the real minimum and maximum values of this hyperrectangle.
	 * Used to confirm a query belongs to this hyperrectangle. As queries might
	 * get smaller during the course of a match, this functionality is necessary
	 *
	 */
	float[][] minMax;

	public float[][] getMinMax() {
		return minMax;
	}

	public void setMinMax(float[][] minMax) {
		this.minMax = minMax;
	}

	public double[] getA() {
		return min;
	}

	public void setA(double[] a) {
		this.min = a;
	}

	public double[] getB() {
		return width;
	}

	public void setB(double[] b) {
		this.width = b;
	}

	public double[] getExp() {
		return exp;
	}

	public void setExp(double[] e) {
		this.exp = e;
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

	public SpaceTreeLeaf search(float[] value) {
		assert pointInside(value);
		// we are in the leaf, we are done
		return this;
	}

	/**
	 * Normalizes the given tuple, puts the result in "result"
	 *
	 * @param value
	 *            (tuple to be normalized)
	 * @param result
	 *            (the resulting tuple)
	 */
	public void normalize(float[] value, float[] result) {
		assert pointInside(value);
		int i = 0;
		while (i < value.length) {
			result[i] = normalizeAux(value[i], i);
			assert result[i] >= 0;
			assert result[i] <= 1;
			i++;
		}
	}

	public float normalizeAux(float value, int i) {
		return (float) Math.pow((value - min[i]) /  width[i], exp[i]);
	}

	/**
	 * Takes a query rectangle and returns all the spaces that intersect with
	 * it. Since this object is a leaf we just add ourselves to the list
	 *
	 * @param query
	 *            the query to be searched
	 * @param result
	 *            will hold all the spaces that intersect with the query
	 */
	public void searchRange(float[][] query, List<SpaceTreeLeaf> result) {
		assert intersects(query); // this has to be true
		//if (intersects(query)) {
		result.add(this);
		//}
	}

	/**
	 * Returns true if the given query intersects with this hyperrectangle
	 *
	 * @param query
	 * @return true if the given query intersects with this hyperrectangle
	 */
	public boolean intersects(float[][] query) {
		boolean res = true;
		assert query.length == minMax.length;
		int i = 0;
		while (i < query.length && res == true) {
			res = intersectsAux(minMax[i], query[i]);
			i++;
		}
		return res;
	}

	public boolean intersectsAux(float[] space, float[] query) {
		assert space.length == 2;
		assert query.length == 2;
		// calculate the only cases where no intersection is found
		return !(query[MIN] > space[MAX] || query[MAX] < space[MIN]);
	}

	/**
	 * Returns true if the given point is inside this space
	 *
	 * @return true if the given point is inside
	 */
	public boolean pointInside(float[] point) {
		int i = 0;
		assert point.length == minMax.length;
		boolean res = true;
		while (i < point.length && res) {
			res = minMax[i][MIN] <= point[i] && point[i] <= minMax[i][MAX];
			i++;
		}
		return res;
	}

	/**
	 * Normalizes the given query to this hyperrectangle Parts of the query
	 * might return values that are greater than 1 or 0. In those cases, we
	 * "cut" the query so that this is not performed
	 *
	 * @param firstPassQuery
	 * @param result
	 */
	/*
	 * public void generateRectangle(float[][] firstPassQuery, float[][]
	 * result){ int i = 0; while(i < firstPassQuery.length){ result[i][MAX] =
	 * normalizeAux(Math.min(firstPassQuery[i][MAX],minMax[i][MAX]),i);
	 * result[i][MIN] =
	 * normalizeAux(Math.max(firstPassQuery[i][MIN],minMax[i][MIN]),i);
	 * //result[i][MIN] = Math.max(normalizeAux(firstPassQuery[i][MIN],i),0);
	 * assert result[i][MAX] <= 1 : " Original: " + firstPassQuery[i][MAX] + "
	 * Generated: " + result[i][MAX] + " \n" + Arrays.deepToString(minMax);
	 * assert result[i][MIN] >= 0 : " Original: " + firstPassQuery[i][MIN] + "
	 * Generated: " + result[i][MIN] + " \n" + Arrays.deepToString(minMax);
	 * assert result[i][MIN] <= result[i][MAX]: "MIN: " + result[i][MIN] + "
	 * MAX: " + result[i][MAX] + " Originals: MIN: " + firstPassQuery[i][MIN] + "
	 * MAX: "+ firstPassQuery[i][MAX] + " \n" + Arrays.deepToString(minMax);
	 * i++; } }
	 */

	/*
	public void generateRectangle(float[][] firstPassQuery, float[][] result) {

		assert intersects(firstPassQuery);

		int i = 0;
		while (i < firstPassQuery.length) {
			// borrowed from Zhang's code
			double one = min[i] * firstPassQuery[i][MIN] - width[i];
			if (one <= 0) {
				result[i][MIN] = 0;
			} else {
				result[i][MIN] = (float) Math.pow(one, exp[i]);
			}

			result[i][MAX] = normalizeAux(firstPassQuery[i][MAX], i);
			if (result[i][MAX] > 1) {
				result[i][MAX] = 1;
			}

			assert result[i][MAX] <= 1 : " Original: " + firstPassQuery[i][MAX]
					+ " Generated: " + result[i][MAX] + " \n"
					+ Arrays.deepToString(minMax);
			assert result[i][MIN] >= 0 : " Original: " + firstPassQuery[i][MIN]
					+ " Generated: " + result[i][MIN] + " \n"
					+ Arrays.deepToString(minMax);
			assert result[i][MIN] <= result[i][MAX] : "MIN: " + result[i][MIN]
					+ " MAX: " + result[i][MAX] + " Originals: MIN: "
					+ firstPassQuery[i][MIN] + " MAX: "
					+ firstPassQuery[i][MAX] + " \n"
					+ Arrays.deepToString(minMax);
			i++;
		}
	}*/

	public void generateRectangle(float[][] firstPassQuery, float[][] result) {
		int i = 0;
		while (i < firstPassQuery.length) {

			float min = firstPassQuery[i][MIN];
			if(min < minMax[i][MIN]){
				min = minMax[i][MIN];
			}
			result[i][MIN] = normalizeAux(min, i);

			float max = firstPassQuery[i][MAX];
			if(max > minMax[i][MAX]){
				max = minMax[i][MAX];
			}
			result[i][MAX] = normalizeAux(max, i);

			assert result[i][MAX] <= 1 : " Original: " + firstPassQuery[i][MAX]
					+ " Generated: " + result[i][MAX] + " \n"
					+ Arrays.deepToString(minMax);
			assert result[i][MIN] >= 0 : " Original: " + firstPassQuery[i][MIN]
					+ " Generated: " + result[i][MIN] + " \n"
					+ Arrays.deepToString(minMax);
			assert result[i][MIN] <= result[i][MAX] : "MIN: " + result[i][MIN]
					+ " MAX: " + result[i][MAX] + " Originals: MIN: "
					+ firstPassQuery[i][MIN] + " MAX: "
					+ firstPassQuery[i][MAX] + " \n"
					+ Arrays.deepToString(minMax);
			i++;
		}
	}

}
