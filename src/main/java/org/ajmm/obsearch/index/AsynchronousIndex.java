package org.ajmm.obsearch.index;

interface AsynchronousIndex {

    /**
     * Returns true if this match is complete
     * @return
     */
    public abstract boolean isFinished();

}