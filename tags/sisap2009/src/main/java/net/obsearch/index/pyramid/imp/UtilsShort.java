package net.obsearch.index.pyramid.imp;

import java.nio.ByteBuffer;

import net.obsearch.OB;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.storage.CloseIterator;

import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.storage.OBStoreDouble;
import net.obsearch.storage.TupleDouble;

/**
 * Utilities for short indexes.
 * @author amuller
 *
 */
public class UtilsShort<O extends OBShort> {
    
    /**
     * This method reads from the B-tree appies l-infinite to discard false
     * positives. This technique is called SMAP. Calculates the real distance
     * and updates the result priority queue It is left public so that junit can
     * perform validations on it Performance-wise this is one of the most
     * important methods
     * @param object
     *                object to search
     * @param tuple
     *                tuple of the object
     * @param r
     *                range
     * @param hlow
     *                lowest pyramid value
     * @param hhigh
     *                highest pyramid value
     * @param result
     *                result of the search operation
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     */
    public static void searchBTreeAndUpdate(final OBShort object, final short[] tuple,
            final short r, final double hlow, final double hhigh,
            final OBPriorityQueueShort < OBShort > result, OBStoreDouble C, IndexShort<OBShort> index)
            throws IllegalAccessException, InstantiationException,
            IllegalIdException, OBException {

        CloseIterator < TupleDouble > it = C.processRange(hlow, hhigh);
        try{
        short max = Short.MIN_VALUE;
        short realDistance = Short.MIN_VALUE;
        while (it.hasNext()) {
            TupleDouble tup = it.next();
            ByteBuffer in = tup.getValue();

            int i = 0;
            short t;
            max = Short.MIN_VALUE;
            while (i < tuple.length) {
                t = (short) Math.abs(tuple[i] - in.getShort());
                if (t > max) {
                    max = t;
                    if (t > r) {
                        break; // finish this loop this slice won't be
                        // matched
                        // after all!
                    }
                }
                i++;
            }
            if (max <= r && result.isCandidate(max)) {
                // there is a chance it is a possible match
                long id = in.getLong();
                OBShort toCompare = index.getObject(id);
                realDistance = object.distance(toCompare);
                if (realDistance <= r) {
                    result.add(id, toCompare, realDistance);
                }
            }
        }
        }finally{
            it.closeCursor();
        }
        
    }

}
