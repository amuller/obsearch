package org.ajmm.obsearch.index.pptree;

import java.util.List;

public interface SpaceTree {

    public static final int MIN = 0;

    public static final int MAX = 1;

    /**
     * Determines if the node is a leaf or not
     * @return
     */
    boolean isLeafNode();

    /**
     * Determines the subspace for the given value I know it is funny that the
     * interface specifies a SpaceTreeLeaf... Is this good? Is this bad? Please
     * post your comments. I just felt that it helps to document better... but
     * it is cyclic.
     * @param value
     * @return
     */
    SpaceTreeLeaf search(float[] value);

    /**
     * Takes a query rectangle and returns all the spaces that intersect with
     * it. The list should be empty the first time you call this method
     * @param query
     *            the query to be searched
     * @param result
     *            will hold all the spaces that intersect with the query
     */
    void searchRange(float[][] query, List < SpaceTreeLeaf > result);

    /**
     * Returns the space tree that holds the corresponding number Please see the
     * comment in "search" method of this interface
     * @param spaceNumber
     * @return The space with number spaceNumber
     */
    SpaceTreeLeaf searchSpace(int spaceNumber);
}
