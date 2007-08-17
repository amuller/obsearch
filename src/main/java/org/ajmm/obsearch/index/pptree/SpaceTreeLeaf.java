package org.ajmm.obsearch.index.pptree;

import java.util.Arrays;
import java.util.List;

public class SpaceTreeLeaf implements SpaceTree {

    /**
     * Space number.
     */
    private int SNo;

    /**
     * Minimum value for the dimension.
     */
    private double[] min;

    /**
     * Width of the dimension.
     */
    private double[] width;

    /**
     * Precalculated value for normalization.
     */
    private double[] exp;

    /**
     * @return A human readable representation of the object.
     */
    public String toString() {
        return "leaf(" + SNo + " " + Arrays.deepToString(minMax) + ")";
    }

    /**
     * This holds the real minimum and maximum values of this hyperrectangle.
     * Used to confirm a query belongs to this hyperrectangle. As queries might
     * get smaller during the course of a match, this functionality is necessary
     */
    private float[][] minMax;

    /**
     * @return the min max values of this hyper rectangle.
     */
    public float[][] getMinMax() {
        return minMax;
    }

    /**
     * sets the min max values of this hyper rectangle.
     * @param minMax
     *            new min max values of this hyper rectangle.
     */
    public void setMinMax(float[][] minMax) {
        this.minMax = minMax;
    }

    /**
     * @return min
     */
    public double[] getA() {
        return min;
    }

    /**
     * Sets min.
     * @param a
     *            new min
     */
    public void setA(double[] a) {
        this.min = a;
    }

    /**
     * @return width
     */
    public double[] getB() {
        return width;
    }

    /**
     * Sets width.
     * @param b
     *            new width
     */
    public void setB(double[] b) {
        this.width = b;
    }

    /**
     * @return exp values for all dimensions
     */
    public double[] getExp() {
        return exp;
    }

    /**
     * Set new exp values for the dimensions.
     * @param e
     *            new exp values for the dimensions
     */
    public void setExp(double[] e) {
        this.exp = e;
    }

    /**
     * @return gets the space number
     */
    public int getSNo() {
        return SNo;
    }

    /**
     * Sets the space number of this method
     * @param no
     *            new space number
     */
    public void setSNo(int no) {
        SNo = no;
    }

    /**
     * @return always returns true because this is a leaf node.
     */
    public boolean isLeafNode() {
        return true;
    }

    /**
     * @param value Point to search
     * @return this if the given value is inside this space
     */
    public SpaceTreeLeaf search(final float[] value) {
        assert pointInside(value);
        // we are in the leaf, we are done
        return this;
    }

    /**
     * Normalizes the given tuple, puts the result in "result".
     * @param value
     *            (tuple to be normalized)
     * @param result
     *            (the resulting tuple)
     */
    public void normalize(final float[] value, final float[] result) {
        assert pointInside(value);
        int i = 0;
        while (i < value.length) {
            result[i] = normalizeAux(value[i], i);
            assert result[i] >= 0;
            assert result[i] <= 1;
            i++;
        }
    }
/**
 * Normalizes the given value in dimension i
 * @param value Value to normalize.
 * @param i Dimension
 * @return Normalized version of the value in dimension i.
 */
    public float normalizeAux(float value, int i) {
        return (float) Math.pow((value - min[i]) / width[i], exp[i]);
    }

    /**
     * Takes a query rectangle and returns all the spaces that intersect with
     * it. Since this object is a leaf we just add ourselves to the list
     * @param query
     *            the query to be searched
     * @param result
     *            will hold all the spaces that intersect with the query
     */
    public void searchRange(float[][] query, List < SpaceTreeLeaf > result) {
        assert intersects(query); // this has to be true
        // if (intersects(query)) {
        result.add(this);
        // }
    }

    /**
     * Returns true if the given query intersects with this hyperrectangle.
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

    /**
     * Function used by {@link #intersects(float[][])} to find
     * if any two dimension ranges are overlapping or not.
     * @param space the dimension range of the space
     * @param query the dimension range of the query
     * @return true if space and query intersect
     */
    private boolean intersectsAux(float[] space, float[] query) {
        assert space.length == 2;
        assert query.length == 2;
        // calculate the only cases where no intersection is found
        return !(query[MIN] > space[MAX] || query[MAX] < space[MIN]);
    }

    /**
     * Returns true if the given point is inside this space.
     * @param point point to be tested
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
     * @param firstPassQuery query first pass-normalized
     * @param result The resulting normalized query for this space.
     */
   public void generateRectangle(float[][] firstPassQuery, float[][] result) {
        int i = 0;
        while (i < firstPassQuery.length) {

            float min = firstPassQuery[i][MIN];
            if (min < minMax[i][MIN]) {
                min = minMax[i][MIN];
            }
            result[i][MIN] = normalizeAux(min, i);

            float max = firstPassQuery[i][MAX];
            if (max > minMax[i][MAX]) {
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
/**
 * @param spaceNumber SNo to search.
 * @return If this object's SNo == spaceNumber we return this, otherwise we
 * return null.
 */
    public SpaceTreeLeaf searchSpace(int spaceNumber) {
        if (this.SNo == spaceNumber) {
            return this;
        } else {
            return null;
        }
    }

}
