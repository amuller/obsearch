package org.ajmm.obsearch.index.pptree;

import java.util.List;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * SpaceTreeNode are non-leaf nodes of the Space Tree.
 * Please see the paper on the P+tree to understand more about this.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class SpaceTreeNode implements SpaceTree {

    /**
     * Division dimension.
     */
    private int DD;

    /**
     * Division value.
     */
    private float DV;

    /**
     * Left node.
     */
    private SpaceTree left;

    /**
     * Right node.
     */
    private SpaceTree right;

    /**
     * @return a string representation of this node.
     */
    @Override
    public String toString() {
        return "[DD: " + DD + " DV " + DV + "](" + left + "," + right + ")";
    }

    /**
     * @return true if this node is a leaf node (always false)
     */
    public final boolean isLeafNode() {
        return false;
    }

    /**
     * @return the DD of the node.
     */
    public final int getDD() {
        return DD;
    }

    /**
     * Sets the DD of the node.
     * @param dd new DD.
     */
    public final void setDD(int dd) {
        DD = dd;
    }

    /**
     * Gets the DV of the node.
     * @return DV of the node.
     */
    public final float getDV() {
        return DV;
    }

    /**
     * Sets the DV of the node.
     * @param dv new dv
     */
    public final void setDV(float dv) {
        DV = dv;
    }

    /**
     * @return left child.
     */
    public final SpaceTree getLeft() {
        return left;
    }

    /**
     * Set left child.
     * @param left new left child.
     */
    public final void setLeft(SpaceTree left) {
        this.left = left;
    }

    /**
     * @return right child.
     */
    public final SpaceTree getRight() {
        return right;
    }

    /**
     * Set right child.
     * @param right new right child.
     */
    public final void setRight(SpaceTree right) {
        this.right = right;
    }


    public final SpaceTreeLeaf search(float[] value) {
        if (value[DD] < DV) {
            return left.search(value);
        }
        if (value[DD] >= DV) {
            return right.search(value);
        } else {
            assert false;
            return null;
        }
    }

    /**
     * Takes a query rectangle and returns all the spaces that intersect with
     * it.
     * @param query
     *            the query to be searched
     * @param result
     *            will hold all the spaces that intersect with the query
     */
    public final void searchRange(float[][] query, List < SpaceTreeLeaf > result) {
        // if the maximum value of the query in the given dimension is lower
        // than the split value, then we can safely ignore the other branches
        if (query[DD][MIN] <= DV) {
            left.searchRange(query, result);
        }

        if (query[DD][MAX] >= DV) {
            right.searchRange(query, result);
        }
    }

    /**
     * Searches for space spaceNumber and returns such leaf.
     * @param spaceNumber the space number we want to search
     * @return The spaceLeaf with SNo spaceNumber
     */
    public final SpaceTreeLeaf searchSpace(int spaceNumber) {
        SpaceTreeLeaf l = right.searchSpace(spaceNumber);
        SpaceTreeLeaf r = left.searchSpace(spaceNumber);
        assert !(l != null && r != null);
        if (l != null) {
            return l;
        }
        if (r != null) {
            return r;
        } else {
            return null;
        }
    }

}
