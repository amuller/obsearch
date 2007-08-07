package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import net.jxta.endpoint.ByteArrayMessageElement;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.Message.ElementIterator;
import net.jxta.exception.PeerGroupException;

import org.ajmm.obsearch.AsynchronousIndex;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.AbstractP2PIndex.PipeHandler;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public class P2PIndexShort<O extends OBShort> extends AbstractP2PIndex<O>
	implements IndexShort<O> {
    
    private static final transient Logger logger = Logger
	.getLogger(P2PIndexShort.class);

    // we keep two different references of the same object
    // to avoid casts
    IndexShort<O> index;

    SynchronizableIndex<O> syncIndex;

    // stores the queries to be used
    protected QueryProcessingShort[] queries;

    /**
         * Creates a P2P Index short The provided index must be
         * SynchronizableIndex and also implement IndexShort. The index must be
         * frozen. The number of boxes served will be the maximum number of
         * boxes serviced by the index
         * 
         * @param index
         * @param dbPath
         *                place were to put p2p related data
         * @param clientName
         *                the name
         * @throws IOException
         * @throws PeerGroupException
         * @throws OBException
         */
    public P2PIndexShort(SynchronizableIndex<O> index, File dbPath,
	    String clientName, int maximumServerThreads) throws IOException,
	    PeerGroupException, OBException, NotFrozenException,
	    DatabaseException {
	this(index, dbPath, clientName, index.totalBoxes(),
		maximumServerThreads);
	queries = new QueryProcessingShort[maximumItemsToProcess];
    }

    protected void queryTimeoutCheckEntry(int tap, long time) {
	if(queries[tap] == null){
	    return;
	}
	synchronized (queries[tap]) {
	    // if the entry is not disabled
	    long t = queries[tap].getLastRequestTime();
	    if(queries[tap].isFinished()){
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
         * Creates a P2P Index short The provided index must be
         * SynchronizableIndex and also implement IndexShort. The index must be
         * frozen.
         * 
         * @param index
         * @param dbPath
         *                place were to put p2p related data
         * @param clientName
         *                the name
         * @param boxesToServe #
         *                of boxes that will be served
         * @throws IOException
         * @throws PeerGroupException
         * @throws OBException
         */
    public P2PIndexShort(SynchronizableIndex<O> index, File dbPath,
	    String clientName, int boxesToServe, int maximumServerThreads)
	    throws IOException, PeerGroupException, OBException,
	    NotFrozenException, DatabaseException {
	super(index, dbPath, clientName, boxesToServe, maximumServerThreads);
	if (!(index instanceof IndexShort)) {
	    throw new OBException("Expecting an IndexShort");
	}
	if (!index.isFrozen()) {
	    throw new NotFrozenException();
	}
	this.index = (IndexShort<O>) index;
	this.syncIndex = index;

    }

    protected SynchronizableIndex<O> getIndex() {
	return syncIndex;
    }

    /**
         * Perform a distributed search in the network
         * 
         * @param object
         * @param r
         * @param result
         * @throws NotFrozenException
         * @throws DatabaseException
         * @throws InstantiationException
         * @throws IllegalIdException
         * @throws IllegalAccessException
         * @throws OutOfRangeException
         * @throws OBException
         */
    public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
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

    public boolean intersects(O object, short r, int box)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	return index.intersects(object, r, box);
    }

    public int[] intersectingBoxes(O object, short r)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	return index.intersectingBoxes(object, r);
    }

    /**
         * This method is not supported for P2P indexes
         */
    public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
	    int[] boxes) throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {

	throw new OBException("This method is not supported");

    }

    private class QueryProcessingShort<T> extends QueryProcessing {

	private OBPriorityQueueShort<O> result;

	private short range;

	public QueryProcessingShort() {
	    this(null, null, (short) -1, -1, null);
	}

	public QueryProcessingShort(int[] x, OBPriorityQueueShort<O> result,
		short range, int tab, O object) {
	    super(x, tab, object);
	    this.result = result;
	    this.range = range;
	}

	public short getRange() {
	    return range;
	}

	public void setRange(short range) {
	    this.range = range;
	}

	public OBPriorityQueueShort<O> getResult() {
	    return result;
	}

	public void setResult(OBPriorityQueueShort<O> result) {
	    this.result = result;
	}

	public void handleResult(Message msg) throws InstantiationException,
		IllegalAccessException, OBException, DatabaseException {
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
		    int[] newBoxes = intersectingBoxes(this.object, newRange);
		    updateRemainingBoxes(newBoxes);
		}
		performNextMatch();
	    }
	    //if(logger.isDebugEnabled()){
		//logger.debug("5) Match was updated or a query was re-sent");
	    //}
	}

	protected void writeRange(TupleOutput out) {
	    out.writeShort(range);
	}

	protected void addResult(Message msg) {
	    addResultAux(result, msg, MessageType.SQ);
	}

	protected boolean rangeChanged() {
	    return result.updateRange(range) != range;
	}

	protected void writeK(TupleOutput out) {
	    assert result != null;
	    out.writeByte(result.getK());
	}

    }

    /**
         * Performs the match, and creates a message that will be sent back to
         * the peer who queried us
         * 
         * <pre>
         * Header: |requestId| |tab| |num_boxes| |box1| |box2|... 
         * Query: |range| |k| |object| 
         * Result: (multiple results) |distance| |object|
         * </pre>
         */
    protected Message performMatch(int[] boxes, Message msg)
	    throws InstantiationException, IllegalAccessException, OBException,
	    DatabaseException {
	// get the object and the parameters
	//Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
	ByteArrayMessageElement m = getMessageElement(msg, MessageType.SQ,
		MessageElementType.SO);
	TupleInput in = new TupleInput(m.getBytes());
	short r = in.readShort();
	byte k = in.readByte();
	O obj = this.index.readObject(in);
	// now we have to read all the candidates that have been processed
	OBPriorityQueueShort<O> result = new OBPriorityQueueShort<O>(k);
	fillResultsFromMessage(msg, MessageType.SQ, result);
	
	// now we can perform the match
	this.index.searchOB(obj, r, result, boxes);
	//if(logger.isDebugEnabled()){
	 //   logger.debug("Match performed: boxes: " + Arrays.toString(boxes) +" res: " + result.toString() );	
	//}
	// match is completed... we have to reply back a message
	// that can be parsed with the peer that requested the information
	Message res = new Message();
	addResultAux(result, res, MessageType.SR);
	return res;
    }

    /**
         * Loads the packages that look like
         * 
         * <pre>
         * Result: (multiple results) |distance| |object|
         * </pre>
         * 
         * from msg to the priority queue result
         * 
         * @param msg
         * @param type
         * @param result
         */
    protected void fillResultsFromMessage(Message msg, MessageType type,
	    OBPriorityQueueShort<O> result) throws InstantiationException,
	    IllegalAccessException, OBException, DatabaseException {
	ElementIterator it = msg.getMessageElements(type.toString(),
		MessageElementType.SSR.toString());
	while (it.hasNext()) {
	    ByteArrayMessageElement h = (ByteArrayMessageElement) it.next();
	    TupleInput in = new TupleInput(h.getBytes());
	    short distance = in.readShort();
	    O o = this.index.readObject(in);
	    // FIXME: remove ids from OBPriorityQueue*
	    result.add(-1, null, distance);
	}
    }

    protected void addResultAux(OBPriorityQueueShort<O> result, Message msg,
	    MessageType x) {
	Iterator<OBResultShort<O>> it = result.iterator();
	while (it.hasNext()) {
	    OBResultShort<O> t = it.next();
	    TupleOutput out = new TupleOutput();
	    out.writeShort(t.getDistance());
	    t.getObject().store(out);
	    // add the message:
	    addMessageElement(MessageElementType.SSR, msg, x, out.toByteArray());
	}
    }

    protected void processMatchResult(int tab, long id, Message msg)
	    throws InstantiationException, IllegalAccessException, OBException,
	    DatabaseException {
	// There can be multiple calls to this tab at any given time
	synchronized (this.queries[tab]) {
	    QueryProcessingShort q = this.queries[tab];
	    // load the new results into the QueryProcessing object
	    // if the request id is not the same something it means that
	    // a timed out request arrived too late (someone else already
	    // worked on the result)
	    if(logger.isDebugEnabled()){
		logger.debug("4) Received result for id:  " + id + " tab: " + id);
	    }
	    if (q.getLastRequestId() == id) {
		q.handleResult(msg);
	    }
	}
    }

    protected QueryProcessing returnQuery(int tab) {
	return (QueryProcessing) queries[tab];
    }
}
