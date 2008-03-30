package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import net.jxta.endpoint.ByteArrayMessageElement;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.Message.ElementIterator;
import net.jxta.exception.PeerGroupException;

import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

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
 * A P2P index for distance functions that return short values.
 * @param <O>
 *            The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class P2PIndexShort < O extends OBShort >
        extends AbstractP2PIndex < O > implements IndexShort < O > {

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(P2PIndexShort.class);

    /**
     * we keep two different references of the same object to avoid casts.
     */
    private IndexShort < O > index;

    /**
     * we keep two different references of the same object to avoid casts.
     */
    private SynchronizableIndex < O > syncIndex;

    /**
     * Stores the queries to be used.
     */
    private QueryProcessingShort[] queries;
    
    public String getSerializedName(){
        return this.getClass().getSimpleName();
    }

    /**
     * Creates a P2P Index short The provided index must be SynchronizableIndex
     * and also implement IndexShort. The index must be frozen. The number of
     * boxes served will be the maximum number of boxes serviced by the index
     * @param index
     *            The index that will be wrapped into this index.
     * @param dbPath
     *            place were to put p2p related data
     * @param clientName
     *            the name
     * @param maximumServerThreads
     *            Max number of threads the index will use (currently it has no
     *            effect)
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws IOException
     *             if dbPath points to an invalid path or the directory cannot
     *             be created.
     * @throws PeerGroupException
     *             JXTA network exception
     */
    public P2PIndexShort(final SynchronizableIndex < O > index,
            final File dbPath, final String clientName,
            final int maximumServerThreads) throws IOException,
            PeerGroupException, OBException, NotFrozenException,
            DatabaseException {
        this(index, dbPath, clientName, index.totalBoxes(),
                maximumServerThreads);
        queries = new QueryProcessingShort[maximumItemsToProcess];
    }

    @Override
    protected void queryTimeoutCheckEntry(final int tap, final long time) {
        if (queries[tap] == null) {
            return;
        }
        synchronized (queries[tap]) {
            // if the entry is not disabled
            long t = queries[tap].getLastRequestTime();
            if (queries[tap].isFinished()) {
                return;
            }
            assert t <= time;
            if (t != -1) {
                if (Math.abs(t - time) > queryTimeout) {
                    // re-send the previous query
                    queries[tap].repeatPreviousQuery();
                }
            }
        }
    }

    /**
     * Creates a P2P Index short The provided index must be SynchronizableIndex
     * and also implement IndexShort. The index must be frozen. The number of
     * boxes served will be the maximum number of boxes serviced by the index
     * @param index
     *            The index that will be wrapped into this index.
     * @param dbPath
     *            place were to put p2p related data
     * @param boxesToServe
     *            The number of boxes that will be served
     * @param clientName
     *            the name
     * @param maximumServerThreads
     *            Max number of threads the index will use (currently it has no
     *            effect)
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws IOException
     *             if dbPath points to an invalid path or the directory cannot
     *             be created.
     * @throws PeerGroupException
     *             JXTA network exception
     */
    public P2PIndexShort(final SynchronizableIndex < O > index,
            final File dbPath, final String clientName, final int boxesToServe,
            final int maximumServerThreads) throws IOException,
            PeerGroupException, OBException, NotFrozenException,
            DatabaseException {
        super(index, dbPath, clientName, boxesToServe, maximumServerThreads);
        if (!(index instanceof IndexShort)) {
            throw new OBException("Expecting an IndexShort");
        }
        if (!index.isFrozen()) {
            throw new NotFrozenException();
        }
        this.index = (IndexShort < O >) index;
        this.syncIndex = index;

    }

    @Override
    protected SynchronizableIndex < O > getIndex() {
        return syncIndex;
    }

    /**
     * Perform a distributed search in the network. This search method will
     * inmediately return. Que query will be searched in background. Users
     * should then check the method {@link #isProcessingQueries()}. When this
     * method returns true, all the pending queries will have been completed.
     * @param object
     *            The object that has to be searched
     * @param r
     *            The range to be used
     * @param result
     *            A priority queue that will hold the result
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws IllegalIdException
     *             This exception is left as a Debug flag. If you receive this
     *             exception please report the problem to:
     *             http://code.google.com/p/obsearch/issues/list
     */
    public final void searchOB(final O object, final short r,
            final OBPriorityQueueShort < O > result) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {
        // 1) get a <tab> (from super. takeATab)
        boolean interrupted = true;
        int tab = -1;
        while (interrupted) {
            try {
                tab = super.takeATab.take();
                interrupted = false;
            } catch (InterruptedException e) {

            }
        }

        // 2) find the boxes that have to be matched for "object"
        int[] boxes = intersectingBoxes(object, r);
        // 3) create a QueryProcessing.
        // to save some unnecessary garbage collection we will reuse the
        // QueryProcessing Objects.
        // synchronize because some lost and old request might try to access the
        // object
        synchronized (queries) {
            if (queries[tab] == null) {
                queries[tab] = new QueryProcessingShort();
            }
        }

        synchronized (queries[tab]) {

            queries[tab].setBoxIndex(0);
            queries[tab].setLastRequestId(-1);
            queries[tab].setLastRequestTime(-1);
            queries[tab].setPrevIndex(0);
            queries[tab].setRemainingBoxes(boxes);
            queries[tab].setTab(tab);
            queries[tab].setResult(result);
            // 4) QueryProcessing will find a peer to match.
            // sends the query to the pipe
            queries[tab].setRange(r);
            queries[tab].setObject(object);
            queries[tab].performNextMatch();
            // 5) after a while, the pipe receives the result (req_id <tab>
            // [dist][ob1] [dist][obj2]...)
            // 6) The pipe gives the message to QueryProcessingShort and
            // internal data is
            // updated, after this, we can
        }
    }

    public boolean intersects(final O object, final short r, final int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        return index.intersects(object, r, box);
    }

    public int[] intersectingBoxes(final O object, final short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        return index.intersectingBoxes(object, r);
    }

    /**
     * <b>This method is not supported for P2P indexes.</b>
     */
    public void searchOB(final O object, final short r,
            final OBPriorityQueueShort < O > result, final int[] boxes)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {

        throw new OBException("This method is not supported");

    }

    /**
     * Utility class that keeps control of query results.
     * @param <T>
     *            Dummy parameter.
     */
    private class QueryProcessingShort < T >
            extends QueryProcessing {

        /**
         * Query result.
         */
        private OBPriorityQueueShort < O > result;

        /**
         * Range to be employed.
         */
        private short range;

        /**
         * Default constructor.
         */
        public QueryProcessingShort() {
            this(null, null, (short) -1, -1, null);
        }

        /**
         * Constructor.
         * @param x
         *            Boxes to process
         * @param result
         *            Result object
         * @param range
         *            Range
         * @param tab
         *            Tab number from the take-a-tab
         * @param object
         *            the object that we will match.
         */
        public QueryProcessingShort(final int[] x,
                final OBPriorityQueueShort < O > result, final short range,
                final int tab, final O object) {
            super(x, tab, object);
            this.result = result;
            this.range = range;
        }

        /**
         * @return current range
         */
        public short getRange() {
            return range;
        }

        /**
         * Sets the range.
         * @param range
         */
        public void setRange(final short range) {
            this.range = range;
        }

        /**
         * @return the current result.
         */
        public OBPriorityQueueShort < O > getResult() {
            return result;
        }

        /**
         * Sets the current result.
         * @param result
         *            The result to set.
         */
        public void setResult(final OBPriorityQueueShort < O > result) {
            this.result = result;
        }

        @Override
        public void handleResult(final Message msg)
                throws InstantiationException, IllegalAccessException,
                OBException, DatabaseException {
            // basically add the MessageElementType.SSR
            // to the results. Find out if the range changed...
            // if it changed, start checking check each box to see
            // if we should add the box again (maybe getting the boxes again
            // is cheaper)
            // once the range and the remaining of the boxes has been
            // calculated
            // perform the match again.
            fillResultsFromMessage(msg, MessageType.SR, result);
            if (isFinished()) {
                // nothing more to do, we just have to free resources
                takeATab.add(tab);
                // put the time in -1; this means the object id disabled
                super.lastRequestTime = -1;
            } else {
                logger.debug("REDOING MATCH");
                // if the range changes, then the boxes change too.
                short newRange = result.updateRange(range);
                if (newRange != range) {
                    // range is smaller now
                    // we should calculate the boxes, and remove all the
                    // boxes we have explored so far.
                    // those are the boxes to the left of boxIndex
                    this.range = newRange;
                    int[] newBoxes = intersectingBoxes(object, newRange);
                    updateRemainingBoxes(newBoxes);
                }
                performNextMatch();
            }
            // if(logger.isDebugEnabled()){
            // logger.debug("5) Match was updated or a query was re-sent");
            // }
        }

        @Override
        protected void writeRange(final TupleOutput out) {
            out.writeShort(range);
        }

        @Override
        protected void addResult(final Message msg) {
            addResultAux(result, msg, MessageType.SQ);
        }

        @Override
        protected boolean rangeChanged() {
            return result.updateRange(range) != range;
        }

        @Override
        protected void writeK(final TupleOutput out) {
            assert result != null;
            out.writeByte(result.getK());
        }

    }

    /**
     * Performs the match, and creates a message that will be sent back to the
     * peer who queried us
     * 
     * <pre>
     * Header: |requestId| |tab| |num_boxes| |box1| |box2|... 
     * Query: |range| |k| |object| 
     * Result: (multiple results) |distance| |object|
     * </pre>
     * 
     * @param boxes
     *            Perform match on the given boxes.
     * @param msg
     *            Use the given message to retrieve additional parameters.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    @Override
    protected Message performMatch(final int[] boxes, final Message msg)
            throws InstantiationException, IllegalAccessException, OBException,
            DatabaseException {
        // get the object and the parameters
        // Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        ByteArrayMessageElement m = getMessageElement(msg, MessageType.SQ,
                MessageElementType.SO);
        TupleInput in = new TupleInput(m.getBytes());
        short r = in.readShort();
        byte k = in.readByte();
        O obj = this.index.readObject(in);
        // now we have to read all the candidates that have been processed
        OBPriorityQueueShort < O > result = new OBPriorityQueueShort < O >(k);
        fillResultsFromMessage(msg, MessageType.SQ, result);

        // now we can perform the match
        this.index.searchOB(obj, r, result, boxes);
        // if(logger.isDebugEnabled()){
        // logger.debug("Match performed: boxes: " + Arrays.toString(boxes) +"
        // res: " + result.toString() );
        // }
        // match is completed... we have to reply back a message
        // that can be parsed with the peer that requested the information
        Message res = new Message();
        addResultAux(result, res, MessageType.SR);
        return res;
    }

    /**
     * Loads object lists and puts them into result.
     * 
     * <pre>
     * Result: (multiple results) |distance| |object|
     * </pre>
     * 
     * from msg and puts them into priority queue result
     * @param msg
     *            The msg from which we will extract the results
     * @param type
     *            The type of message (query or query response)
     * @param result
     *            The matcheswill be stored in result.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    protected final void fillResultsFromMessage(final Message msg,
            final MessageType type, final OBPriorityQueueShort < O > result)
            throws InstantiationException, IllegalAccessException, OBException,
            DatabaseException {
        ElementIterator it = msg.getMessageElements(type.toString(),
                MessageElementType.SSR.toString());
        while (it.hasNext()) {
            ByteArrayMessageElement h = (ByteArrayMessageElement) it.next();
            TupleInput in = new TupleInput(h.getBytes());
            short distance = in.readShort();
            O o = this.index.readObject(in);
            // FIXME: remove ids from OBPriorityQueue*
            result.add(-1, o, distance);
        }
    }

    /**
     * Aux function for
     * {@link #fillResultsFromMessage(Message, org.ajmm.obsearch.index.AbstractP2PIndex.MessageType, OBPriorityQueueShort)}.
     * Extracts from msg elements of type x and loads the objects putting them
     * into result.
     * @param result
     *            Where the objects will be stored
     * @param msg
     *            Extract the elements from this message
     * @param x
     *            Use the given message type.
     */
    protected void addResultAux(OBPriorityQueueShort < O > result,
            final Message msg, final MessageType x) {
        Iterator < OBResultShort < O >> it = result.iterator();
        while (it.hasNext()) {
            OBResultShort < O > t = it.next();
            TupleOutput out = new TupleOutput();
            out.writeShort(t.getDistance());
            t.getObject().store(out);
            // add the message:
            addMessageElement(MessageElementType.SSR, msg, x, out.toByteArray());
        }
    }

    @Override
    protected void processMatchResult(final int tab, final long id,
            final Message msg) throws InstantiationException,
            IllegalAccessException, OBException, DatabaseException {
        // There can be multiple calls to this tab at any given time
        synchronized (this.queries[tab]) {
            QueryProcessingShort q = this.queries[tab];
            // load the new results into the QueryProcessing object
            // if the request id is not the same something it means that
            // a timed out request arrived too late (someone else already
            // worked on the result)
            // if(logger.isDebugEnabled()){
            // logger.debug("4) Received result for id: " + id + " tab: " + id);
            // }
            if (q.getLastRequestId() == id) {
                q.handleResult(msg);
            }
        }
    }

    /**
     * Return the given query (a tab from take-a-tab).
     * @param tab
     *            The tab we want
     * @return The object associated to the given tab.
     */
    protected QueryProcessing returnQuery(final int tab) {
        return (QueryProcessing) queries[tab];
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Index#getStats()
     */
    @Override
    public String getStats() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Index#resetStats()
     */
    @Override
    public void resetStats() {
        // TODO Auto-generated method stub
        
    }
    
    
}
