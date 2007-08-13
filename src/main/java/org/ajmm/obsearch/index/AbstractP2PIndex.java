package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.document.MimeMediaType;
import net.jxta.endpoint.ByteArrayMessageElement;
import net.jxta.endpoint.EndpointAddress;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.MessageElement;
import net.jxta.endpoint.Message.ElementIterator;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.ID;
import net.jxta.id.IDFactory;
import net.jxta.peergroup.PeerGroup;
import net.jxta.pipe.PipeID;
import net.jxta.pipe.PipeMsgEvent;
import net.jxta.pipe.PipeMsgListener;
import net.jxta.pipe.PipeService;
import net.jxta.platform.NetworkConfigurator;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PeerAdvertisement;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.util.JxtaBiDiPipe;
import net.jxta.util.JxtaServerPipe;

import org.ajmm.obsearch.AsynchronousIndex;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.TimeStampResult;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.BoxNotAvailableException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.ajmm.obsearch.index.AbstractSynchronizableIndex.TimeStampIterator;
import org.ajmm.obsearch.index.utils.Directory;
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
 * AbstractP2PIndex holds common functionality of indexes that span several
 * computers. The current implementation uses the JXTA library as network
 * infraestructure.
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public abstract class AbstractP2PIndex<O extends OB> implements Index<O>,
	DiscoveryListener, AsynchronousIndex {

    /**
         * Lists all the message types available in the network
         * 
         */
    public static enum MessageType {
	TIME, // time
	BOX, // box information (for sync and box selection purposes)
	SYNCBOX, // synchronize box (request for synchronization)
	SYNCC, // synchronize continue (asks for more data)
	SYNCR, // synchronize retry (asks for the last packet in the event of a timeout)
	SYNCE, // synchronize end (ends the synchronization for one box)
	INDEX, // local index data
	INSOB, // insert object (after a SYNCBOX)
	DSYNQ, // data sync query
	DSYNR, // data sync reply
	SQ, // search query
	SR
	// search result
    };

    /**
         * Lists all the message element types available (these elements are
         * components found in MessageType packets)
         */
    public static enum MessageElementType {
	SH, /** search header */
	SO, /** search object */
	SSR
	/** search sub result */
    };

    private transient final Logger logger;

    // messages bigger than this one have problems
    private static final int messageSize = 40 * 1024;

    // the string that holds the original index xml
    protected String indexXml;

    // min number of pivots to have at any time... for controlling purposes
    // the necessary minimum number of peers to allow matching might be
    // bigger
    // than this.
    protected static final int minNumberOfPeers = 5;

    // JXTA variables
    private transient NetworkManager manager;

    private transient DiscoveryService discovery;

    private String clientName;

    private final static String pipeName = "OBSearchPipe";

    private final static int maxNumberOfPeers = 100;

    // Interval for each heartbeat (in miliseconds)
    // heartbeats check for missing resources and make sure we are all well
    // connected all the time
    private final static int heartBeatInterval = 10000;

    // general timeout used for most p2p operations
    private final static int globalTimeout = 30 * 1000;

    // maximum time difference between the peers.
    // peers that have bigger time differences will be dropped.
    private final static int maxTimeDifference = 3600000;

    // maximum number of objects to query at the same time
    protected static final int maximumItemsToProcess = 15;

    // maximum time to wait for a query to be answered.
    protected static final int queryTimeout = 30000;

    // internally gives ids that can be used to get a space in the queries
    // array
    // this "tab" can be used to refer to the result until it is completed
    protected BlockingQueue<Integer> takeATab;

    // maximum number of objects to match at the same time.
    // this should be close to the amount of CPUs available
    protected int maximumServerThreads;

    // object in charge of accepting new connections
    private JxtaServerPipe serverPipe;

    // contains a pipe per client that have tried to connect to us or that
    // we
    // have tried to connect to
    // the key is a peer id and not a pipe!
    private ConcurrentMap<String, PipeHandler> clients;

    // time when the index was created
    protected long indexTime;

    private PeerGroup netPeerGroup;

    private File dbPath;

    private String timer = "time";

    private PipeAdvertisement adv;

    protected abstract SynchronizableIndex<O> getIndex();

    // The boxes that we are currently serving
    private int[] ourBoxes;

    // total number of boxes that will be supported
    private int boxesCount;

    // the minimum amount of boxes that will be served
    private int boxesToServe;

    // Each entry has a List. An entry in the array equals to a box #.
    // Every List holds handlers that hold the box in which they are indexed
    // each handler is responsible of registering and unregistering
    private List<List<PipeHandler>> handlersPerBox;

    // keeps track of the latest handler that was used for each box.
    // at search time this values are used to distribute the queries
    // "evenly"
    private AtomicIntegerArray handlerSearchIndexes;

    // set to true when some data is inserted or deleted or
    // when a new peer comes in
    // set to false when we publish all the peers our information
    // or when a new peer comes in.
    private AtomicBoolean boxesUpdated;

    private boolean isClient = true;

    // a unique identifier of requests.
    private AtomicLong requestId;

    // threads to be used in the search
    private Semaphore searchThreads;

    // it is set to true when data is inserted or deleted into this index.
    // once the available boxes are published, this variable returns to
    // false.
    private AtomicBoolean modifiedData;

    // tells if we are syncing or not.
    // when we finish syncing the last box
    private AtomicBoolean syncing;

    // syncing this box (index in the ourBoxes array)
    private PipeHandler syncingPipe;

    // last time we requested a box sync
    private AtomicLong syncingBoxLastRequestTime;

    /**
         * Initialize the abstract class
         * 
         * @param index
         *                the index that will be distributed
         * @param dbPath
         *                the path where we will store information related to
         *                this index
         * @param boxesToServe
         *                The number of boxes that will be served by this index
         * @throws IOException
         * @throws PeerGroupException
         * @throws NotFrozenException
         */
    protected AbstractP2PIndex(SynchronizableIndex<O> index, File dbPath,
	    String clientName, int boxesToServe, int maximumServerThreads)
	    throws IOException, PeerGroupException, NotFrozenException,
	    DatabaseException, OBException {
	if (!index.isFrozen()) {
	    throw new NotFrozenException();
	}
	if (!dbPath.exists()) {
	    throw new IOException(dbPath + " does not exit");
	}
	this.maximumServerThreads = maximumServerThreads;
	clients = new ConcurrentHashMap<String, PipeHandler>();
	boxesCount = index.totalBoxes();
	this.dbPath = dbPath;
	this.clientName = clientName;
	this.boxesToServe = boxesToServe;
	this.boxesUpdated = new AtomicBoolean(false);
	logger = Logger.getLogger(clientName);

	handlersPerBox = Collections
		.synchronizedList(new ArrayList<List<PipeHandler>>(index
			.totalBoxes()));
	initHandlersPerBox(handlersPerBox);

	// initialize the boxes this index is supporting if the given index
	// has the corresponding data.
	if (index.databaseSize() != 0) { // if the database has some
                                                // data, we serve the data
	    // of the db
	    int i = 0;
	    List<Integer> boxes = new ArrayList<Integer>(index.totalBoxes());
	    while (i < index.totalBoxes()) {
		if (index.latestModification(i) != -1) {
		    boxes.add(i);
		}
		i++;
	    }
	    this.ourBoxes = new int[boxes.size()];
	    i = 0;
	    while (i < boxes.size()) {
		ourBoxes[i] = boxes.get(i);
		i++;
	    }
	} else {
	    decideServicedBoxes(boxesToServe, index);
	}

	takeATab = new ArrayBlockingQueue<Integer>(maximumItemsToProcess);
	int i = 0;
	while (i < maximumItemsToProcess) {
	    takeATab.add(i);
	    i++;
	}

	handlerSearchIndexes = new AtomicIntegerArray(boxesCount);

	requestId = new AtomicLong(0);

	searchThreads = new Semaphore(maximumServerThreads, true);

	this.modifiedData = new AtomicBoolean(false);

	syncing = new AtomicBoolean(false);
	syncingPipe = null;
	syncingBoxLastRequestTime = new AtomicLong(-1);
    }

    // makes sure that all the components of the given list of lists are
    // synchronized
    private void initHandlersPerBox(List<List<PipeHandler>> x) {
	int i = 0;
	while (i < boxesCount) {
	    x.add(Collections.synchronizedList(new ArrayList<PipeHandler>(
		    maxNumberOfPeers)));
	    i++;
	}
    }

    /**
         * Returns true if any box == 0. This means we need sync But only if we
         * are connected to providers of every box TODO: make this method
         * realize that he has to sync if the timestamps of the other peers are
         * newer
         * 
         * @return
         */
    private boolean needSync() throws OBException, DatabaseException {
	if (syncing.get()) {
	    // we won't sync again if we are syncing already
	    //logger.debug("no need to sync because we are syncing");
	    return false;
	}
	if (ourBoxes == null) {
	    return false;
	}

	int i = 0;
	while (i < ourBoxes.length) {
	    int box = ourBoxes[i];
	    if (needSyncInBox(box)) {
		 // logger.debug("box " + box + " should be synced");
		return true;
	    }
	    i++;
	}
	return false;
    }

    /**
         * Returns true if I have to sync box i or false otherwise
         * 
         * @param i
         *                (box #)
         * @return
         */
    private boolean needSyncInBox(int i) throws OBException, DatabaseException {
	long boxTime = getIndex().latestModification(i);

	PipeHandler mostRecent = mostRencentPipeHandlerPerBox(i);
	if (mostRecent == null) {
	    return false; // we should wait until we get peers that serve
	    // this box
	}
	assert mostRecent.isServing(i);
	long mr = mostRecent.lastUpdated(i);
	//logger.debug(" Most recent box " + mr + " box Time " + boxTime);
	
	return  mr > boxTime;
    }

    /**
         * Returns the pipe handler whose ith box has the most recent
         * modification time
         * 
         * @param i
         * @return
         */
    private PipeHandler mostRencentPipeHandlerPerBox(int i) {
	List<PipeHandler> boxList = handlersPerBox.get(i);
	long time = -1;
	PipeHandler ph = null;
	Iterator<PipeHandler> it = boxList.iterator();
	while (it.hasNext()) {
	    PipeHandler p = it.next();
	    assert p.isServing(i);
	    if (time < p.lastUpdated(i)) {
		time = p.lastUpdated(i);
		ph = p;
	    }
	}
	return ph;
    }

    /**
         * Initializes the p2p network
         * 
         * @param
         * @throws IOException
         * @throws PeerGroupException
         */
    private void init(boolean isClient, NetworkManager.ConfigMode c,
	    boolean clearCache, URI seedURI) throws IOException,
	    PeerGroupException {

	File cache = new File(new File(dbPath, ".cache"), clientName);
	if (clearCache) {
	    Directory.deleteDirectory(cache);
	}
	manager = new NetworkManager(c, clientName, cache.toURI());
	NetworkConfigurator configurator = manager.getConfigurator();
	// clear the seeds
	configurator.setRendezvousSeedURIs(new LinkedList<String>());
	configurator.setRelaySeedURIs(new LinkedList<String>());
	configurator.setRelaySeedingURIs(new HashSet<String>());
	// end of clear the seeds

	configurator.addRdvSeedingURI(seedURI);
	configurator.addRelaySeedingURI(seedURI);
	configurator.setUseOnlyRelaySeeds(true);
	configurator.setUseOnlyRendezvousSeeds(true);
	configurator.setHttpEnabled(false);
	configurator.setTcpIncoming(true);
	configurator.setTcpEnabled(true);
	configurator.setTcpOutgoing(true);
	configurator.setUseMulticast(false);
	manager.startNetwork();
	// Get the NetPeerGroup
	netPeerGroup = manager.getNetPeerGroup();

	// get the discovery service
	discovery = netPeerGroup.getDiscoveryService();
	discovery.addDiscoveryListener(this);
	adv = getPipeAdvertisement();
	// init the incoming connection listener
	serverPipe = new JxtaServerPipe(netPeerGroup, adv);
	serverPipe.setPipeTimeout(0);

	// TODO: learn why we can't put the following 4 lines
	// after getting rendezvous connection...
	discovery.publish(adv);
	discovery.remotePublish(adv);
	discovery.publish(netPeerGroup.getPeerAdvertisement());
	discovery.remotePublish(netPeerGroup.getPeerAdvertisement());

	// wait for rendevouz connection
	if (isClient) {
	    logger.debug("Waiting for rendevouz connection");
	    manager.waitForRendezvousConnection(0);
	    logger.debug("Rendevouz connection found");
	}
	if (logger.isDebugEnabled()) {
	    logger.debug("Peer id: "
		    + netPeerGroup.getPeerAdvertisement().getPeerID().toURI());
	}

	assert netPeerGroup.getPeerAdvertisement().equals(
		netPeerGroup.getPeerAdvertisement());
    }

    /**
         * This class performs a heartbeat. It makes sure that all the resources
         * we need to properly work.
         * 
         * 
         */
    private class HeartBeat implements Runnable {

	private boolean error = false;

	/**
         * This method starts network connections and calls heartbeat
         * undefinitely until the program stops
         */
	public void run() {
	    long count = 0;
	    while (!error) {

		try {
		    heartBeat1();
		    heartBeat3(count);
		    heartBeat6(count);
		    heartBeat10(count);
		    heartBeat100(count);
		    synchronized (timer) {
			timer.wait(heartBeatInterval);
		    }
		} catch (InterruptedException i) {
		    if (logger.isDebugEnabled()) {
			logger.debug("HeartBeat interrupted");
		    }
		} catch (Exception e) {
		    error = true;
		    logger.fatal("Exception in heartBeat", e);
		    assert false;
		}
		count++;
	    }
	    if (error) {
		logger.fatal("Stopping heartbeat because of error");
		assert false;
	    }
	}

	// executed once per heart beat
	public void heartBeat1() throws PeerGroupException, IOException,
		OBException, DatabaseException {
	    if (needSync()) {
		// lower
		sync();
	    }

	}

	public void heartBeat10(long count) throws DatabaseException,
		IOException, OBException, PeerGroupException {
	    if (count % 10 == 0) {
		// find pipes if not enough peers are available
		// or if not all the boxes have been covered
		if (!minimumNumberOfPeers() || !totalBoxesCovered()) {
		    // logger.debug("Finding pipes!");
		    findPipes();
		}
		// check timeouts
		// queryTimeoutCheck();
	    }
	}

	public void heartBeat6(long count) throws PeerGroupException,
		IOException, OBException, DatabaseException {
	    if (count % 6 == 0) {

	    }
	}

	// executed once every 3 heart beats
	public void heartBeat3(long count) throws PeerGroupException,
		IOException, OBException, DatabaseException {
	    if (count % 3 == 0) {

		sendBoxInfo();
		info();

		// check for timeouts in the sync process
		syncAlive();
	    }
	}

	// executed once every 100 heartbeats
	public void heartBeat100(long count) throws PeerGroupException,
		IOException, OBException, DatabaseException {
	    if (count % 100 == 0) {
		// advertisements should be proactively searched for if we are
		// running out
		// of connections
		// sync();
		timeBeat();
	    }
	}
    }

    /**
         * Returns true if the index is still processing query results
         * 
         * @return
         */
    public boolean isProcessingQueries() {
	return takeATab.size() != maximumItemsToProcess;
    }

    /**
         * Monitors all the
         * 
         */
    protected void queryTimeoutCheck() {
	if (isProcessingQueries()) {
	    long time = System.currentTimeMillis();
	    int i = 0;
	    while (i < maximumItemsToProcess) {
		queryTimeoutCheckEntry(i, time);
		i++;
	    }
	}
    }

    /**
         * 
         * @param tap
         * @param time
         */
    protected abstract void queryTimeoutCheckEntry(int tap, long time);

    /**
         * Prerequisites: Each PipeHandler contains information of the latest
         * modification of each of its served boxes. The sync method has to
         * accomplish several things: 1) Compare the latest updates performed by
         * other pipes and if there is someone with a more recent update, ask
         * for the data 2) Decide if we shall serve other boxes depending on the
         * desired amount of boxes to serve and the number of boxes currently
         * served by the peers that surround us.
         */
    private void sync() throws OBException, DatabaseException, IOException {
	logger.debug("sync");
	int i = 0;
	while (i < boxesCount) {
	    int box = ourBoxes[i];
	    if (needSyncInBox(box)) {
		// start syncing from this box:
		PipeHandler ph = mostRencentPipeHandlerPerBox(i);
		// we should send the lastestModification - 1
		// millisecond to be safe
		ph.sendRequestSyncMessage(i,
			getIndex().latestModification(box) - 1);
		// do this only for one box.
		break;
	    }
	    i++;
	}

    }

    /**
         * Prints some information of the peer
         */
    private void info() throws OBException, DatabaseException {
	int[] servicedBoxes = servicedBoxes();
	if (servicedBoxes != null) {
	    int[] boxCount = new int[servicedBoxes.length];
	    long[] times = new long[servicedBoxes.length];
	    int i = 0;
	    while (i < boxCount.length) {
		boxCount[i] = getIndex().elementsPerBox(i);
		
                 if(logger.isDebugEnabled()){ times[i] =
                 getIndex().latestModification(i); }
                 
		i++;
	    }
	    logger.info("Heart: Connected Peers: " + clients.size() + " B: "
		    + Arrays.toString(ourBoxes) + ", boxes: "
		    + Arrays.toString(boxCount));
	     if(logger.isDebugEnabled()){
	     logger.debug("Latest modifications:" +
                 Arrays.toString(times));
	     }
	} else {
	    logger.info("Heart: Connected Peers: " + clients.size()
		    + " no boxes being served. ");
	}
    }

    /**
         * Returns the pipe handler that has the reported most recent
         * modification Returns null if we don't have a peer with the given box.
         * 
         * @param box
         * @return
         */
    private PipeHandler latestPipeHandler(int box) {
	PipeHandler res = null;
	long time = 0;
	Iterator<PipeHandler> it = this.handlersPerBox.get(box).iterator();
	while (it.hasNext()) {
	    PipeHandler p = it.next();
	    long ltime = p.lastUpdated(box);
	    if (time < ltime) {
		res = p;
		time = ltime;
	    }
	}
	return res;
    }

    private void syncAlive() throws IOException, DatabaseException, OBException {
	if (syncing.get()
		&& (System.currentTimeMillis() - this.syncingBoxLastRequestTime
			.get()) > globalTimeout) {
	    if (syncingPipe != null) {
		logger.debug("Re-syncing after timeout");
		this.syncingPipe.sendReSyncMessage();
	    }

	}
    }

    /**
         * Receives the # of elements per box, and: If we are not serving any
         * box, it finds this.minBoxesToServe boxes. It will select boxes whose
         * count is the least. If we are serving boxes, and we are serving a box
         * that is exceedingly being served, then
         * 
         * @param boxesToServe #
         *                of boxes that will be served
         * @param index
         *                The index used to find out the total amount of boxes
         *                available Changes the internal ourBoxes array with the
         *                boxes that will be served by this index
         */
    private void decideServicedBoxes(int boxesToServe,
	    SynchronizableIndex<O> index) {
	int[] sb = this.servicedBoxes();
	Random r = new Random(System.currentTimeMillis());
	if (sb == null) {
	    sb = new int[boxesToServe];
	    // select random boxes and make sure no
	    // repeated boxes are selected
	    int i = 0;
	    while (i < sb.length) {
		int nbox = r.nextInt(index.totalBoxes());
		while (inArray(nbox, i, sb)) {
		    // generate a new random until the value
		    // is not in the array
		    nbox = r.nextInt(index.totalBoxes());
		}
		sb[i] = nbox;
		i++;
	    }
	    Arrays.sort(sb);
	}
	this.ourBoxes = sb;
    }

    // returns true if x is found in arr
    // in the interval [0,i[
    private boolean inArray(int x, int i, int[] arr) {
	int cx = 0;
	while (cx < i) {
	    if (arr[cx] == x) {
		return true;
	    }
	    cx++;
	}
	return false;
    }

    /*
         * Receives the # of elements per box, and: If we are not serving any
         * box, it finds this.minBoxesToServe boxes. It will select boxes whose
         * count is the least. If we are serving boxes, and we are serving a box
         * that is exceedingly being served, then
         * 
         * @param boxCount
         */
    /*
         * private void decideServicedBoxes() throws Exception {
         * 
         * int[] sb = this.servicedBoxes(); // initialize box counts int i = 0;
         * int[] boxCount = new int[getIndex().totalBoxes()]; while (i <
         * boxCount.length) { boxCount[i] = this.handlersPerBox.get(i).size();
         * i++; } // include our boxes i = 0; while(i < sb.length){ int box =
         * sb[i]; boxCount[box]++; i++; }
         * 
         * int findBoxes = 1; // number of boxes to find if (sb == null) {
         * findBoxes = minBoxesToServe; } i = 0; while (i < findBoxes) { // find
         * one box that should be replaced int badBox = findWorstBox(sb,
         * boxCount); if(badBox == -1){ break; } // find the box that should not
         * be served // returns the index that should be removed // this cannot
         * leave any box in 0. // if it can't be avoided then we signal an
         * exception int myGoodBox = findBestBox(boxCount); int toRemoveBox =
         * sb[myGoodBox]; sb[myGoodBox] = badBox; // updates boxCount to reflect
         * the new layout boxCount[toRemoveBox]--; boxCount[badBox]++; i++; }
         * this.ourBoxes = sb; assert false: "need to send an update to
         * everybody"; } // find which is the worst box in the locality // the
         * worst box must not be included in myBoxes // returns -1 if there are
         * no worst cases private int findWorstBox(int[] boxCount){ int minVal =
         * Integer.MAX_VALUE; int minIndex = -1; }
         */

    /**
         * For each pipe in pipes, send a time message
         * 
         */
    private void timeBeat() throws IOException {
	logger.debug("time");
	synchronized (clients) {
	    Iterator<PipeHandler> it = this.clients.values().iterator();
	    while (it.hasNext()) {
		PipeHandler u = it.next();
		u.sendTimeMessage();
	    }
	}
    }

    /**
         * Sends box information to all the peers if box information has been
         * changed
         */
    private void sendBoxInfo() throws IOException, DatabaseException,
	    OBException {

	synchronized (clients) {
	    Iterator<PipeHandler> it = clients.values().iterator();
	    while (it.hasNext()) {
		PipeHandler u = it.next();
		u.sendBoxMessage();
	    }
	}

    }

    /**
         * Send the given message to the pipeID
         * 
         * @param pipeID
         * @param msg
         * @throws IOException
         */
    /*
         * protected final void sendMessage(URI pipeID, Message msg) throws
         * IOException { PipeHandler p = clients.get(pipeID); if (p != null) {
         * p.sendMessage(msg); } else { assert false; } }
         */

    /**
         * For every pipe we have registered, we send them the globalBoxCount so
         * that everybody has an overall idea on how many boxes are being served
         * currently. We only need to call this method when: 1) We have changed
         * the set of boxes we are serving 2) someone is telling us that they
         * have done the same, and their information is more recent than ours.
         */
    protected void syncGlobalBoxesInformation() {

    }

    protected boolean minimumNumberOfPeers() {
	return this.clients.size() >= this.minNumberOfPeers;
    }

    public int getNumberOfPeers() {
	return this.clients.size();
    }

    /**
         * Returns true if all the peers have data that is synchronized to the
         * same timestamp. This should not be used normally but it is useful for
         * testing purposes. TODO: fix this sot hat boxes are only sent when we
         * modify one of our boxes
         * 
         * @return
         */
    public boolean areAllPeersSynchronizedWithMe() throws OBException,
	    DatabaseException {
	// browse each box of the
	int i = 0;
	long[] boxes = new long[boxesCount];
	while (i < boxesCount) {
	    long box = getIndex().latestModification(i);
	    if (box != -1) {
		if (boxes[i] == 0) {
		    boxes[i] = box;
		} else if (boxes[i] != box) {
		    return false;
		}
	    }
	    // get the boxes from
	    List<PipeHandler> b = handlersPerBox.get(i);
	    Iterator<PipeHandler> it = b.iterator();
	    while (it.hasNext()) {
		PipeHandler p = it.next();
		if (p.isServing(i)) {
		    box = p.lastUpdated(i);
		    if (boxes[i] == 0) {
			boxes[i] = box;
		    } else if (boxes[i] != box) {
			return false;
		    }
		    // }
		}
	    }
	    i++;
	}
	return true;
    }

    /**
         * Check if we have peers who serve at least one box per box #
         * 
         * @return true if the above condition applies
         */
    public boolean areAllBoxesAvailable() {

	Iterator<List<PipeHandler>> it = handlersPerBox.iterator();
	while (it.hasNext()) {
	    List<PipeHandler> l = it.next();
	    if (l.size() == 0) {
		return false;
	    }
	}
	return true;
    }

    /**
         * This method must be called by all users once It starts the network,
         * and creates some background threads like the hearbeat and the
         * incoming connection handler
         * 
         * @param client
         *                If true, the index will be created in client mode
         *                (from the p2p network perspective) If false, the index
         *                will be a "server".
         * @param clearPeerCache
         *                If we should clear network related cache information
         * @param seedFile
         *                The seed file to be used for this index. Only the
         *                given seeds will be used
         */
    public void open(boolean client, boolean clearPeerCache, File seedFile)
	    throws IOException, PeerGroupException {
	this.isClient = client;
	NetworkManager.ConfigMode c = null;
	if (!seedFile.exists()) {
	    throw new IOException("File does not exist: " + seedFile);
	}
	if (client) {
	    c = NetworkManager.ConfigMode.EDGE;
	} else {
	    c = NetworkManager.ConfigMode.RENDEZVOUS_RELAY;
	}
	URI seedURI = seedFile.toURI();
	// initialize JXTA
	init(client, c, clearPeerCache, seedURI);

	logger.debug("Starting heart...");
	Thread thread = new Thread(new HeartBeat(), "Heart Beat Thread");
	thread.start();
	logger.debug("Starting incoming connections server...");
	Thread thread2 = new Thread(new IncomingConnectionHandler(),
		"Incoming connection Thread");
	thread2.start();
    }

    private PipeAdvertisement getPipeAdvertisement() {
	PipeID pipeID = (PipeID) ID.create(generatePipeID());

	PipeAdvertisement advertisement = (PipeAdvertisement) AdvertisementFactory
		.newAdvertisement(PipeAdvertisement.getAdvertisementType());

	advertisement.setPipeID(pipeID);
	advertisement.setType(PipeService.UnicastType);
	advertisement.setName(pipeName);
	return advertisement;
    }

    private URI generatePipeID() {
	return IDFactory.newPipeID(netPeerGroup.getPeerGroupID()).toURI();
    }

    /** Finds the pipe for the given peer * */
    protected void findPipePeer(URI peer) throws IOException,
	    PeerGroupException {
	if (!minimumNumberOfPeers()) {
	    discovery.getRemoteAdvertisements(peer.toString(),
		    DiscoveryService.ADV, "Name", pipeName, 1, null);

	}
    }

    // query the discovery service for OBSearch pipes
    protected void findPipes() throws IOException, PeerGroupException {
	// logger.debug("Getting advertisements");
	if (isClient) {
	    Enumeration<Advertisement> en = discovery.getLocalAdvertisements(
		    DiscoveryService.PEER, null, null);
	    while (en.hasMoreElements()) {
		Advertisement adv = en.nextElement();
		assert adv instanceof PeerAdvertisement;
		PeerAdvertisement padv = (PeerAdvertisement) adv;
		synchronized (clients) {
		    if (!clients.containsKey(padv.getPeerID())) {
			findPipePeer(padv.getPeerID().toURI());
		    }
		}
	    }
	    discovery.getRemoteAdvertisements(null, DiscoveryService.PEER,
		    null, null, 5, null);
	}

    }

    // check if all the boxes are being supplied
    protected boolean totalBoxesCovered() {
	if (handlersPerBox == null) {
	    return false;
	}
	int i = 0;
	int max = getIndex().totalBoxes();

	while (i < max) {
	    if (this.handlersPerBox.get(i).size() == 0) {
		return false;
	    }
	    i++;
	}
	return true;
    }

    /**
         * Method called by the discovery server, when we discover something
         */
    public void discoveryEvent(DiscoveryEvent ev) {

	try {
	    DiscoveryResponseMsg res = ev.getResponse();

	    Advertisement adv;
	    Enumeration en = res.getAdvertisements();
	    // logger.debug("Discovery event" + ev);
	    if (en != null) {
		while (en.hasMoreElements()) {
		    adv = (Advertisement) en.nextElement();
		    // logger.info("Discovery event: " + adv);
		    if (adv instanceof PipeAdvertisement) {

			synchronized (clients) {
			    // add the pipe only if its peer hasn't been
			    // added
			    // this is a hack in order to prevent additions
			    // of
			    // pipes whose
			    // TODO: if there is some inconsistency with the
			    // string generated by EndpointAddress,
			    // this hack won't work
			    String hack = "urn:"
				    + ((EndpointAddress) ev.getSource())
					    .toURI().toString().replaceAll("/",
						    "");
			    URI u = new URI(hack);
			    // avoid connecting to ourselves and connecting
			    // to
			    // peers we have already
			    if (!u.toString().equals(
				    this.netPeerGroup.getPeerAdvertisement()
					    .getPeerID().toURI().toString())
				    && !isConnectedToPeer(u.toString())) {
				logger.info("Discovered new peer: " + u);
				PipeAdvertisement p = (PipeAdvertisement) adv;
				addPipe(u, p);
			    }
			}
		    } else if (adv instanceof PeerAdvertisement) {
			// if we get a peer advertisement, then if the peer is
			// not yet connected to us
			// we can search for pipes.
			synchronized (clients) {
			    PeerAdvertisement p = (PeerAdvertisement) adv;
			    if (!isConnectedToPeer(p.getPeerID().toURI()
				    .toString())) {
				// find pipes
				findPipePeer(p.getPeerID().toURI());
			    }
			}
		    }
		}
	    }
	} catch (Exception e) {
	    logger.fatal("Exception", e);
	    assert false;
	}
    }

    /**
         * Returns true if the given peer id is connected to us.
         * 
         * @param id
         * @return
         */
    private boolean isConnectedToPeer(String id) {
	return clients.containsKey(id);
    }

    /**
         * Adds the given pipe to our cache of pipes. The pipe is added if our
         * quote of pipes is under the minimum. and if we don't have the pipe
         * already
         * 
         * @param peerID
         *                peerId of the given peer advertisement
         * @param p
         *                peer advertisement to be added
         */

    private void addPipe(URI id, PipeAdvertisement p) {

	try {
	    if (!isConnectedToPeer(id.toString())) {
		PipeHandler ph = new PipeHandler(p);
		assert id.equals(ph.getPeerID());
		addPipeAux(ph.getPeerID(), ph);
	    }
	} catch (IOException e) {
	    logger.warn("Exception while connecting: ", e);
	} catch (Exception e) {
	    logger.fatal("Error while trying to add Pipe:" + p + " \n ", e);
	    assert false;
	}

    }

    /**
         * Adds the given pipe. This is called either when an accept is made by
         * the server, and when the discovery returns from the server.
         * 
         * @param id
         *                (peer id of the given bidipipe)
         * @param ph
         * @throws IOException
         */
    private void addPipeAux(URI id, PipeHandler ph) throws IOException,
	    Exception {
	synchronized (clients) {
	    if (isConnectedToPeer(id.toString())) {

		try {
		    logger
			    .debug("Closing because we are already connected to it: "
				    + id);
		    ph.close();
		}

		catch (IOException e) {
		    logger
			    .fatal("Error while trying to close a duplicated pipe"
				    + e);
		    assert false;
		}

	    } else if (!isConnectedToPeer(id.toString())
		    && clients.size() <= maxNumberOfPeers) {
		logger.debug("Adding pipe of peer: " + ph.getPeerID());
		this.clients.put(id.toString(), ph);
		// send initial sync data to make sure that everybody is
		// syncrhonized enough to be meaningful.
		ph.sendMessagesAfterFirstEncounter();
		boxesUpdated.set(true);
	    } else {
		try {
		    logger.debug("Closing (2) " + id);
		    ph.close();
		} catch (IOException e) {
		    logger.fatal("Error while closing pipe" + e);
		    assert false;
		}
	    }
	}
    }

    /**
         * TODO: Remove synchronized ?
         * 
         * @param bidipipe
         */
    private void addPipe(PipeHandler bidipipe) throws IOException, Exception {
	synchronized (clients) {
	    URI id = bidipipe.getPeerID();
	    addPipeAux(id, bidipipe);
	}
    }

    /**
         * Returns the currently boxes served by the library
         * 
         * @return
         * @throws OBException
         */
    private int[] servicedBoxes() {
	return this.ourBoxes;
    }

    /**
         * Closes the given pipe. all resources are released.
         */
    private void closePipe(String id) throws IOException {
	synchronized (clients) {
	    PipeHandler pipe = clients.get(id);
	    clients.remove(id);
	    pipe.close();
	}
    }

    protected class PipeHandler implements PipeMsgListener {

	private JxtaBiDiPipe pipe;

	// the biggest time difference found
	private long biggestTimeDifference = Long.MIN_VALUE;

	private URI peerID;

	// This holds the last time the boxes of the underlying pipe
	// were updated. The peer is responsible of telling us when
	// he changed.
	// value of 0 means that the box is not being served
	private AtomicLongArray boxLastUpdated;

	// we keep track of the latest sent timestamp in order to
	// avoid re-sending data. YAY! (minimizes duplicated data
	// transmission considerably)
	private AtomicLong[] lastSentTimestamp;

	// we keep track of the most recent SYNCBOX request
	// after a SYNCBOX request several SYNCC (SYNC Continue)
	// messages will be sent until we send the receiver a
	// SYNCE (SYNC End) message
	private TimeStampIterator insertIterator = null;

	// latest sync msg is stored here, in the event of a
	// timeout msg from the peer at the other end, we
	// just resend the bytes stored here.
	private TupleOutput  syncRetry;

	public PipeHandler() {
	    boxLastUpdated = new AtomicLongArray(getIndex().totalBoxes());
	    lastSentTimestamp = new AtomicLong[boxesCount];
	    int i = 0;
	    while (i < lastSentTimestamp.length) {
		lastSentTimestamp[i] = new AtomicLong(-2);
		i++;
	    }
	}

	/**
         * Creates a new pipe handler
         * 
         * @param id
         *                The original peer id
         * @param pipe
         *                The pipe we will
         */
	public PipeHandler(URI id, JxtaBiDiPipe pipe) {
	    this();
	    synchronized (pipe) {
		this.pipe = pipe;
		this.peerID = id;
		pipe.setMessageListener(this);
	    }
	}

	public PipeHandler(PipeAdvertisement p) throws IOException {
	    this();

	    pipe = new JxtaBiDiPipe();

	    //pipe.setReliable(true);
	    synchronized (pipe) {
		pipe.connect(netPeerGroup, null, p, globalTimeout, this);
		peerID = pipe.getRemotePeerAdvertisement().getPeerID().toURI();
	    }
	}

	public URI getPeerID() {
	    return peerID;
	}

	/**
         * Sends a sync request to the underlying pipe. The pipe will
         * asyncrhonally respond with all the objects that he has ed or deleted
         * from "time" and only for the given box Does not send the message if
         * the given time is
         * 
         * @param boxIndex
         *                the index of the box in the ourBoxes array
         * @param time
         * @throws BoxNotAvailableException
         * @throws IOException
         */
	public void sendRequestSyncMessage(int boxIndex, long time)
		throws BoxNotAvailableException, IOException {

	    int box = ourBoxes[boxIndex];

	    syncing.set(true);
	    syncingPipe = this;
	    syncingBoxLastRequestTime.set(System.currentTimeMillis());

	    if (boxLastUpdated.get(box) == 0) {
		// this should not be happening.
		throw new BoxNotAvailableException();
	    }

	    if (logger.isDebugEnabled()) {
		logger.debug("req sync " + box + " " + time);
	    }

	    TupleOutput out = new TupleOutput();
	    out.writeInt(box);
	    out.writeLong(time);
	    Message msg = new Message();
	    addMessageElement(msg, MessageType.SYNCBOX, out.getBufferBytes());
	    sendMessage(msg);
	    // keep track of some global data used to control the sync
	    // process

	}

	/**
         * Sends to the underlying pipe, the following data: | n (int) | | box #
         * (int) | latest_modification (long) |_1 | box # (int) |
         * latest_modification (long) |_2 ... | box # (int) |
         * latest_modification (long) |_n
         * 
         * Where: box #: box identification number object_count: the number of
         * objects in the given box latest_modification: the most recent
         * modification time. n: total number of servied boxes.
         */
	// TODO: This method
	// * should only be invoked when: 1) We just connected with the peer 2)
	// * our data has been modified
	public void sendBoxMessage() throws DatabaseException, OBException,
		IOException {

	    SynchronizableIndex<O> index = getIndex();
	    int[] serviced = servicedBoxes();
	    if (serviced == null) { // this case can happen
		return;
	    }
	    int i = 0;
	    TupleOutput out = new TupleOutput();
	    out.writeInt(serviced.length);
	    /*
                 * if (logger.isDebugEnabled()) { logger.debug("box info " +
                 * Arrays.toString(serviced)); }
                 */
	    while (i < serviced.length) {
		int box = serviced[i];
		long latest = index.latestModification(box);
		out.writeInt(box);
		out.writeLong(latest);
		i++;
	    }
	    Message msg = new Message();
	    addMessageElement(msg, MessageType.BOX, out.getBufferBytes());
	    sendMessage(msg);
	}

	/**
         * When the peers encounter each other for the first time, we send a set
         * of standard sync messages only sent to the given bidipipe. This is to
         * make sure that from the beginning we have performed all the standard
         * sync steps. The same syncs will be performed at various frequencies
         * by the heart.
         * 
         * @param bidipipe
         *                The pipe that to which we will send messages.
         */
	public void sendMessagesAfterFirstEncounter() throws IOException,
		DatabaseException, OBException {
	    if (logger.isDebugEnabled()) {
		logger.debug("Sending messages after first encounter to "
			+ peerID);
	    }
	    sendTimeMessage();
	    sendBoxMessage();
	}

	private final Message makeTimeMessage() throws IOException {
	    return makeTimeMessageAux(System.currentTimeMillis());
	}

	/**
         * Sends a time message to the underlying pipe
         * 
         * @throws IOException
         */
	public final void sendTimeMessage() throws IOException {
	    // time sync message
	    sendMessage(makeTimeMessage());
	}

	private final Message makeTimeMessageAux(long time) throws IOException {
	    TupleOutput out = new TupleOutput();
	    out.writeLong(time);
	    Message msg = new Message();
	    addMessageElement(msg, MessageType.TIME, out.getBufferBytes());
	    return msg;
	}

	private final long parseTimeMessage(Message msg) {
	    ByteArrayMessageElement m = getMessageElement(msg, MessageType.TIME);
	    TupleInput in = new TupleInput(m.getBytes());
	    return in.readLong();
	}

	/**
         * This method is called when a message comes into our pipe.
         */
	public void pipeMsgEvent(PipeMsgEvent event) {

	    Message msg = event.getMessage();
	    if (msg == null) {
		return;
	    }
	    // ************************************************
	    // We handle here all the different messages we will receive.
	    // ************************************************
	    try {
		Iterator<String> it = msg.getMessageNamespaces();
		while (it.hasNext()) {
		    String namespace = it.next();
		    if (namespace.equals("jxta")) {
			continue;
		    }

		    try {
			MessageType messageType = MessageType
				.valueOf(namespace);

			switch (messageType) {
			case TIME:
			    processTime(msg);
			    break;
			case BOX:
			    processBox(msg);
			    break;
			case SYNCBOX:
			    processSyncBox(msg);
			    break;
			case SYNCC:
			    sendNextSyncMessage();
			    break;
			case SYNCE:
			    processSyncEnd();
			    break;
			case SYNCR:
			    processSyncRetry();
			    break;
			case INSOB:
			    processInsertOB(msg);
			    break;
			case SQ:
			    processSearchQuery(msg);
			    break;
			case SR:
			    processSearchResponse(msg);
			    break;
			default:
			    assert false;
			}
		    } catch (IllegalArgumentException i) {
			// we got a message that is not ours
			// /logger.error("Strange message" + msg);
		    }

		}
	    } catch (IOException io) {
		logger.fatal("Exception while receiving message", io);
		assert false;
	    } catch (Exception e) {
		logger.fatal("Exception while receiving message", e);
		assert false;
	    }
	}
	
	 private void processSyncRetry() throws IOException, OBException, DatabaseException{
	     logger.debug("Doing re-sync");
	     sendInsertMessageFromTuple(syncRetry);
	 }

	private void resetSyncInfo() {
	    syncing.set(false);
	    syncingPipe = null;
	    syncingBoxLastRequestTime.set(-1);
	}
	
	public void sendReSyncMessage() throws IOException{
	    Message msg = new Message();
	    // logger.debug("Give me more data!");
	    addMessageElement(msg, MessageType.SYNCR, new byte[1]);
	    sendMessage(msg);
	    updateSyncInfo();
	}
	
	private void updateSyncInfo(){
	    syncing.set(true);
	    syncingPipe = this;
	    syncingBoxLastRequestTime.set(System.currentTimeMillis());
	}

	private void processSyncEnd() throws OBException, DatabaseException,
		IOException {
	    resetSyncInfo();
	    logger.debug("Sync finished for one box");	  
	}

	/**
         * A message with a query result has been returned. The message has the
         * shape:
         * 
         * <pre>
         * Header: |requestId| |tab|
         * Result:
         * (multiple results) |distance| |object|
         * </pre>
         * 
         * @param msg
         */
	private void processSearchResponse(Message msg)
		throws InstantiationException, IllegalAccessException,
		OBException, DatabaseException {
	    ByteArrayMessageElement elem = getMessageElement(msg,
		    MessageType.SR, MessageElementType.SH);
	    TupleInput in = new TupleInput(elem.getBytes());
	    long id = in.readLong();
	    int tab = in.readInt();

	    // we have to send back the results.
	    processMatchResult(tab, id, msg);
	}

	/**
         * Process a search query message. Query the database and return to the
         * caller the result. The input message goes like:
         * 
         * <pre>
         *  
         * Header: |requestId| |tab|
         * |num_boxes| |box1| |box2|... 
         * Query: |range| |k| |object| 
         * Result:
         * (multiple results) |distance| |object|
         * </pre>
         * 
         * The message that is returned to the caller looks like:
         * 
         * <pre>
         * Header: |requestId| |tab|
         * Result:
         * (multiple results) |distance| |object|
         * </pre>
         * 
         */
	private void processSearchQuery(Message msg) throws IOException,
		InstantiationException, IllegalAccessException, OBException,
		DatabaseException {
	    // searchThreads.acquireUninterruptibly();
	    long t = System.currentTimeMillis();
	    ByteArrayMessageElement m = getMessageElement(msg, MessageType.SQ,
		    MessageElementType.SH);
	    TupleInput in = new TupleInput(m.getBytes());
	    long id = in.readLong();
	    // needed just to help on the other side...
	    // this tab comes from the other peer
	    int tab = in.readInt();

	    // extract boxes that will be matched
	    int boxNum = in.readInt();
	    int[] boxes = new int[boxNum];
	    int i = 0;
	    while (i < boxNum) {
		boxes[i] = in.readInt();
		i++;
	    }
	    // if(logger.isDebugEnabled()){
	    // logger.debug("2) Query request id:" + id + " tab: " +tab + "
	    // boxes: " + Arrays.toString(boxes));
	    // }
	    // extract the
	    Message toSender = performMatch(boxes, msg);
	    // we need to add the id and the tab
	    TupleOutput out = new TupleOutput();
	    out.writeLong(id);
	    out.writeInt(tab);
	    addMessageElement(MessageElementType.SH, toSender, MessageType.SR,
		    out.toByteArray());
	    // send the result back:
	    sendMessage(toSender);
	    // searchThreads.release();
	    // logger.info("3) Took: " + ((System.currentTimeMillis() - t))
	    // + " mseconds to match an object. tab: " + tab + " id: " +
	    // id);
	}

	/**
         * The message comes with an object, and also the original date the
         * object was inserted if we don't use this date, we will keep reloading
         * data over and over again. Example: A received some data (time 1) A
         * sends the data to B, (from time 3) A thinks that B has new data, and
         * receives again the records.
         * 
         * @param msg
         * @throws InstantiationException
         * @throws IllegalAccessException
         * @throws OBException
         * @throws DatabaseException
         */
	private void processInsertOB(Message msg)
		throws InstantiationException, IllegalAccessException,
		OBException, DatabaseException, IOException {
	    ElementIterator it = msg
		    .getMessageElementsOfNamespace(MessageType.INSOB.toString());
	    int i = 0;
	    long time = -2;
	    // logger.debug("Starting insertion");
	    long t = System.currentTimeMillis();
	    while (it.hasNext()) {
		ByteArrayMessageElement m = (ByteArrayMessageElement) it.next();
		TupleInput in = new TupleInput(m.getBytes());
		try {
		    int cx = 0;
		    while (true) {
			time = in.readLong();
			if (time == 0) {
			    // we are done when we find a time of 0
			    break;
			}
			O o = readObject(in);
			// logger.info("Inserting object");
			getIndex().insert(o, time);
			cx++;
		    }
		    logger.debug("Inserted objects: " + cx + " in "
			    + (System.currentTimeMillis() - t) + " msec");
		} catch (IndexOutOfBoundsException e) {
		    // we are done
		}
		i++;
	    }
	    assert i == 1;
	    boxesUpdated.set(true);

	    sendSyncCMsg();
	}

	private void sendSyncCMsg() throws IOException {
	    Message msg = new Message();
	    // logger.debug("Give me more data!");
	    addMessageElement(msg, MessageType.SYNCC, new byte[1]);
	    sendMessage(msg);	    
	    updateSyncInfo();
	}

	/**
         * Responds to a sync request. We initilalize some internal structures
         * used to keep
         * 
         */
	private void processSyncBox(Message msg) throws OBException,
		DatabaseException, IOException {

	    ByteArrayMessageElement m = getMessageElement(msg,
		    MessageType.SYNCBOX);
	    TupleInput in = new TupleInput(m.getBytes());
	    int box = in.readInt();
	    long time = in.readLong();

	    if (insertIterator != null) {
		// we have to close the iterator so that the cursor resources
		// can
		// be recovered
		synchronized (insertIterator) {
		    logger.info("WARNING, writing on an opened iterator");
		    // insertIterator.close();
		}
	    }
	    // now we just have to read the latest modifications and send
	    // them
	    // one by one to the underlying pipe.
	    insertIterator = (TimeStampIterator) getIndex()
		    .elementsInsertedNewerThan(box, time);

	    sendNextSyncMessage();
	}

	private void sendNextSyncMessage() throws IOException,
		DatabaseException, OBException {
	    // we will write here all the bytes
	    if(insertIterator == null){
		// return because we have to wait for an "official"
		// sync request based on something
		sendEndSyncMessage();
		return;
	    }
	    synchronized (insertIterator) {
		TupleOutput out = new TupleOutput();
		int i = 0;
		while (insertIterator.hasNext()) {
		    TimeStampResult<O> r = insertIterator.next();
		    O o = r.getObject();
		    // format: <time> <data>
		    long t = r.getTimestamp();
		    out.writeLong(t);
		    o.store(out);
		    i++;
		    if ((out.getBufferLength()) > messageSize) {
			break;
		    }

		}
		out.writeLong(0);
		sendInsertMessageFromTuple(out);
		// store the tuple for rsync
		syncRetry = out;
	    }
	    if (!insertIterator.hasNext()) {

		// no more boxes found, end the synchronization
		// the peer at the other end will call sync again
		// and another box will be processed
		sendEndSyncMessage();
		insertIterator = null;

	    }

	}

	private void sendEndSyncMessage() throws IOException {
	    Message msg = new Message();
	    addMessageElement(msg, MessageType.SYNCE, new byte[1]);
	    sendMessage(msg);
	}

	/**
         * Sends the given tupleoutput.
         * 
         * @param count
         * @param out
         * @throws OBException
         * @throws DatabaseException
         * @throws IOException
         */
	private void sendInsertMessageFromTuple(TupleOutput out)
		throws OBException, DatabaseException, IOException {
	    Message minsert = new Message();
	    byte[] data = out.getBufferBytes();

	    if (data.length > 0) {
		addMessageElement(minsert, MessageType.INSOB, data);
		this.sendMessage(minsert);
	    }
	}

	/**
         * Process the given msg that should contain a BOX element we leave our
         * heart the decision of getting data from the underlying Peer. TODO:
         * this method is inneficient on purpose. We want to have it as correct
         * as possible. We don't expect this method to be called a lot, so it
         * should ok for now.
         */
	private void processBox(Message msg) {
	    ByteArrayMessageElement m = getMessageElement(msg, MessageType.BOX);
	    assert m != null;
	    TupleInput in = new TupleInput(m.getBytes());
	    int totalBoxes = in.readInt();
	    int i = 0;
	    // this creates a little contention, there are better ways of
	    // doing
	    // it, but for the meantime it is safe.
	    synchronized (handlersPerBox) {
		// remove all the references of this.
		this.removeFromHandler();
		// update the boxes that were sent in the message
		while (i < totalBoxes) {
		    int box = in.readInt();
		    long time = in.readLong();
		    this.boxLastUpdated.set(box, time);
		    handlersPerBox.get(box).add(this);
		    i++;
		}
	    }
	}

	/**
         * Returns the latest updated information for the given box
         * 
         * @param box
         * @return latest updated information for the given box
         */
	public long lastUpdated(int box) {
	    assert box >= 0 && box <= boxesCount : "Box inputted:" + box;
	    return this.boxLastUpdated.get(box);
	}

	/**
         * Returns true if the given box is being served by this peer
         * 
         * @param box
         * @return
         */
	public boolean isServing(int box) {
	    return this.boxLastUpdated.get(box) != 0;
	}

	/**
         * Sends a message to the underlying pipe
         * 
         * @param msg
         * @throws IOException
         */
	public void sendMessage(Message msg) throws IOException {
	    try {
		if (pipe != null) {
		    synchronized (pipe) {
			this.pipe.sendMessage(msg);
		    }
		} else {
		    logger
			    .warn("Could not send message because pipe was closed");
		}

	    } catch (IOException e) {
		logger.warn("IOException when sending a message");
		closePipe(this.peerID.toString());
		throw e;
	    } catch (Exception e) {
		// if this happens, is because the pipe was closed in the other
		// end.
		// we just have to close this connection
		logger.warn("Closing pipe because received an exception. "
			+ this.peerID, e);
		closePipe(this.peerID.toString());

	    }
	}

	/**
         * Recevies a message that contains a time MessageElement
         * 
         * @param msg
         * @param peerID
         *                (the pipe we received the
         */
	private void processTime(Message msg) throws IOException {
	    long time = parseTimeMessage(msg);
	    long ourtime = System.currentTimeMillis();
	    long diff = time - ourtime;
	    // if (logger.isDebugEnabled()) {
	    // logger.debug("Received time " + time + " from peer: " +
	    // peerID
	    // + " diff: " + diff);
	    // }
	    // time must be within +- k units away from our current time.
	    // otherwise we drop the connection with the given pipe id
	    if (Math.abs(diff) > maxTimeDifference) {
		logger.debug("Rejected " + time + " from peer: " + peerID
			+ " because clocks are too skewed!");
		closePipe(peerID.toString());
	    }
	    if (Math.abs(biggestTimeDifference) < Math.abs(diff)) {
		this.biggestTimeDifference = diff;
	    }
	}

	/**
         * Closes the underlying pipe and releases results Removes the
         */
	public void close() throws IOException {
	    synchronized (pipe) {
		synchronized (handlersPerBox) {
		    pipe.close();
		    pipe = null;
		    removeFromHandler();
		}
	    }
	}

	private void removeFromHandler() {
	    int i = 0;
	    // remove all the references of this.
	    while (i < boxLastUpdated.length()) {
		boxLastUpdated.set(i, 0);
		// shifts all the elements to the
		handlersPerBox.get(i).remove(this);
		i++;
	    }
	}

    }

    // End of pipe handler class

    /**
         * This thread is always listening to incoming pipe connections. A new
         * PipeHandler object is created if we decide to accept the incoming
         * connection request.
         */
    private class IncomingConnectionHandler implements Runnable {

	public IncomingConnectionHandler() {

	}

	public void run() {

	    logger.debug("Waiting for connections");
	    while (true) {
		try {
		    JxtaBiDiPipe bidipipe = serverPipe.accept();
		    // add the pipe to our list of pipes, if we can hold
		    // more
		    // people.
		    URI id = bidipipe.getRemotePeerAdvertisement().getPeerID()
			    .toURI();
		    PipeHandler ph = new PipeHandler(id, bidipipe);
		    addPipe(ph);
		} catch (IOException e) {
		    logger.fatal("IO Error while listening to a connection", e);
		    assert false;

		} catch (Exception e) {

		    logger.fatal("Error while listening to a connection", e);
		    assert false;
		}
	    }
	}

    }

    public void close() throws DatabaseException {
	manager.stopNetwork();
	getIndex().close();
	// FIXME: stop all the threads
    }

    public int databaseSize() throws DatabaseException {
	return getIndex().databaseSize();
    }

    public int delete(O object) throws NotFrozenException, DatabaseException {
	int res = getIndex().delete(object);

	this.boxesUpdated.set(true);
	return res;
    }

    public void freeze() throws IOException, AlreadyFrozenException,
	    IllegalIdException, IllegalAccessException, InstantiationException,
	    DatabaseException, OutOfRangeException, OBException,
	    UndefinedPivotsException {
	getIndex().freeze();

    }

    public int getBox(O object) throws OBException {
	return getIndex().getBox(object);
    }

    public O getObject(int i) throws DatabaseException, IllegalIdException,
	    IllegalAccessException, InstantiationException, OBException {
	return getIndex().getObject(i);
    }

    public int insert(O object) throws IllegalIdException, DatabaseException,
	    OBException, IllegalAccessException, InstantiationException {

	int res = getIndex().insert(object);
	boxesUpdated.set(true);
	return res;
    }

    public boolean isFrozen() {
	return getIndex().isFrozen();
    }

    public int totalBoxes() {
	return getIndex().totalBoxes();
    }

    public void relocateInitialize(File dbPath) throws DatabaseException,
	    NotFrozenException, DatabaseException, IllegalAccessException,
	    InstantiationException, OBException, IOException {
	getIndex().relocateInitialize(dbPath);
    }

    /**
         * Returns the xml of the index embedded in this P2PIndex
         */
    public String toXML() {
	return getIndex().toXML();
    }

    public O readObject(TupleInput in) throws InstantiationException,
	    IllegalAccessException, OBException {
	return getIndex().readObject(in);
    }

    public boolean exists(O object) throws DatabaseException, OBException,
	    IllegalAccessException, InstantiationException {
	return getIndex().exists(object);
    }

    /**
         * This class holds information used to process each query
         */
    protected abstract class QueryProcessing {
	// last request performed
	protected long lastRequestId;

	// last request timestamp (to perform timeouts)
	protected long lastRequestTime;

	// contains the remaining boxes
	protected int[] remainingBoxes;

	// holds the index that points the next boxes that have to be explored
	protected int boxIndex;

	// contains the previous index just in case we have to re-send the
	// request
	protected int prevIndex;

	// tab obtained, implicit in the array but we keep because it is
	// pysicologically
	// more comfortable to have it (for me AJMM)
	protected int tab;

	protected O object;

	public O getObject() {
	    return object;
	}

	public void setObject(O object) {
	    this.object = object;
	}

	public QueryProcessing(int[] remainingBoxes, int tab, O object) {
	    super();
	    this.remainingBoxes = remainingBoxes;
	    boxIndex = 0;
	    lastRequestId = -1;
	    lastRequestTime = -1;
	    this.tab = tab;
	    this.object = object;
	}

	public int getBoxIndex() {
	    return boxIndex;
	}

	public void setBoxIndex(int boxIndex) {
	    this.boxIndex = boxIndex;
	}

	public long getLastRequestId() {
	    return lastRequestId;
	}

	public void setLastRequestId(long lastRequestId) {
	    this.lastRequestId = lastRequestId;
	}

	public long getLastRequestTime() {
	    return lastRequestTime;
	}

	public void setLastRequestTime(long lastRequestTime) {
	    this.lastRequestTime = lastRequestTime;
	}

	public int[] getRemainingBoxes() {
	    return remainingBoxes;
	}

	public void setRemainingBoxes(int[] remainingBoxes) {
	    this.remainingBoxes = remainingBoxes;
	}

	public int getPrevIndex() {
	    return prevIndex;
	}

	public void setPrevIndex(int prevIndex) {
	    this.prevIndex = prevIndex;
	}

	public int getTab() {
	    return tab;
	}

	public void setTab(int tab) {
	    this.tab = tab;
	}

	/*
         * (non-Javadoc)
         * 
         * @see org.ajmm.obsearch.index.AsynchronousIndex#isFinished()
         */
	protected boolean isFinished() {
	    return this.remainingBoxes.length >= boxIndex;
	}

	/**
         * Updates the remainingBoxes array Removes the boxes already processed
         * 
         * @param newBoxes
         *                (this array is destroyed by this method)
         */
	protected void updateRemainingBoxes(int[] newBoxes) {
	    List<Integer> res = new LinkedList<Integer>();
	    int i = 0;
	    while (i < newBoxes.length) {
		if (!exists(newBoxes[i], this.remainingBoxes, this.boxIndex)) {
		    res.add(newBoxes[i]);
		}
		i++;
	    }
	    boxIndex = 0;
	    prevIndex = 0;
	    remainingBoxes = new int[res.size()];
	    i = 0;
	    Iterator<Integer> it = res.iterator();
	    while (it.hasNext()) {
		remainingBoxes[i] = it.next();
		i++;
	    }
	}

	/**
         * Returns true if x is in array. We search array for indexes <
         * max_index
         * 
         * @param x
         * @param array
         * @param max_index
         * @return
         */
	private boolean exists(int x, int[] array, int max_index) {
	    int i = 0;
	    assert array.length >= max_index;
	    while (i < max_index) {
		if (array[i] == x) {
		    return true;
		}
		i++;
	    }
	    return false;
	}

	/**
         * Handles the result sent by the peer who answered the query internally
         * updates the state of the query result. If the match is complete, the
         * method releases the tab and leaves the space open for another entry.
         */
	public abstract void handleResult(Message msg)
		throws InstantiationException, IllegalAccessException,
		OBException, DatabaseException;

	protected PipeHandler findNextPipeHandler() {
	    int nextBox = remainingBoxes[boxIndex];
	    List<PipeHandler> hl = handlersPerBox.get(nextBox);
	    synchronized (hl) {
		// nobody is touching the atomic array for box nextBox
		// we only touch it here!
		int index = handlerSearchIndexes.getAndIncrement(nextBox);
		if (index >= hl.size()) {
		    index = 0;
		    handlerSearchIndexes.set(nextBox, index);
		}
		return hl.get(index);
	    }
	}

	/**
         * Finds the boxes that must be searched. The current selection
         * procedure favors Index efficiency instead of network bandwith this
         * means that we only match boxes that are in ph and that are continuous
         * to the current box.
         * 
         * @param ph
         * @return
         */
	protected List<Integer> findBoxesToSearch(PipeHandler ph) {
	    // get the boxes this pipe handler is serving and determine
	    // which boxes will be matched in the given pipe handler
	    // based on the current boxes to match.
	    int i = boxIndex;
	    List<Integer> res = new LinkedList<Integer>();
	    while (i < remainingBoxes.length) {
		if (ph.isServing(remainingBoxes[i])) {
		    res.add(remainingBoxes[i]);
		} else {
		    // remove this break to search all the
		    // boxes that the ph holds (that are in our list of
		    // boxes)
		    break;
		}
		i++;
	    }

	    return res;
	}

	protected abstract boolean rangeChanged();

	/**
         * Executes the next match step Finds a peer who holds the next boxes to
         * be matched and asks the peer to perform the match The message goes
         * like:
         * 
         * <pre>
         * Header: |requestId| |tab| |num_boxes| |box1| |box2|... 
         * Query: |range| |k| |object| 
         * Result: (multiple results) |distance| |object|
         * </pre>
         */
	public void performNextMatch() {
	    // if(this.isFinished()){
	    // return;
	    // }
	    int prevInd = prevIndex;
	    int boxInd = boxIndex;
	    boolean error = true;
	    while (error) {
		PipeHandler ph = findNextPipeHandler();
		List<Integer> boxesToSearch = findBoxesToSearch(ph);

		prevIndex = boxIndex;
		boxIndex = boxIndex + boxesToSearch.size();

		try {
		    // if(logger.isDebugEnabled()){
		    // logger.debug("1) Querying boxes: " + boxesToSearch +
		    // " for tab:" + this.tab + " id: " +
		    // this.getLastRequestId());
		    // }
		    ph.sendMessage(createCurrentQueryMessage(boxesToSearch));
		    error = false;
		} catch (IOException e) {
		    // we have to retry everything
		    // at this stage the pipe that is broken will have been
		    // erased so the next call of findBoxesToSearch (in the
		    // upper part of the loop) will return another peer
		    error = true;
		    prevIndex = prevInd;
		    boxIndex = boxInd;
		}
	    }
	}

	/**
         * Repeats the previous query Should be used by the heart if there is a
         * timeout error in the
         */
	public void repeatPreviousQuery() {
	    boolean error = true;
	    while (error) {
		boxIndex = prevIndex;
		PipeHandler ph = findNextPipeHandler();
		List<Integer> boxesToSearch = findBoxesToSearch(ph);
		try {
		    ph.sendMessage(createCurrentQueryMessage(boxesToSearch));
		    error = false;
		} catch (IOException e) {
		    // we have to retry everything
		    // at this stage the pipe that is broken will have been
		    // erased so the next call of findBoxesToSearch (in the
		    // upper part of the loop) will return another peer
		    error = true;
		}
	    }
	}

	/**
         * Send query information Uses the current state of the object. Header:
         * long (lastRequestId)| int (box #) | int (box id)...
         * 
         */
	protected Message createCurrentQueryMessage(List<Integer> boxesToSearch) {
	    // now we just have to send the query message
	    lastRequestId = requestId.getAndIncrement();
	    lastRequestTime = System.currentTimeMillis();
	    Message msg = new Message();
	    // create header
	    TupleOutput out = new TupleOutput();
	    out.writeLong(lastRequestId);
	    out.writeInt(tab);
	    // add the boxes to search
	    out.writeInt(boxesToSearch.size());
	    Iterator<Integer> it = boxesToSearch.iterator();
	    while (it.hasNext()) {
		out.writeInt(it.next());
	    }
	    addMessageElement(MessageElementType.SH, msg, MessageType.SQ, out
		    .toByteArray());
	    // create query
	    out = new TupleOutput();
	    writeRange(out);
	    writeK(out);
	    object.store(out);
	    addMessageElement(MessageElementType.SO, msg, MessageType.SQ, out
		    .toByteArray());
	    // create the results
	    addResult(msg);
	    // send the message
	    return msg;
	}

	/**
         * Writes the K of the query.
         * 
         * @param out
         */
	protected abstract void writeK(TupleOutput out);

	/**
         * Writes down the current range employed in the search
         * 
         * @param out
         */
	protected abstract void writeRange(TupleOutput out);

	/**
         * Writes the currently found objects and their respective ranges to the
         * message
         * 
         * @param msg
         */
	protected abstract void addResult(Message msg);
    }

    /**
         * This method must obtain the object to be matched and other parameters
         * from the given message and search the underlying index. The returning
         * message is ready to be transmitted to the peer that requested the
         * query.
         * 
         * @param boxes
         * @param msg
         * @return
         */
    protected abstract Message performMatch(int[] boxes, Message msg)
	    throws InstantiationException, IllegalAccessException, OBException,
	    DatabaseException;

    /**
         * Extracts the message associated to the given namespace Assumes that
         * the message only contains one element
         * 
         * @param msg
         * @param namespace
         * @return The ByteArrayMessageElement associated to the only
         *         MessageElement in this Message
         */
    public final static ByteArrayMessageElement getMessageElement(Message msg,
	    MessageType namespace) {
	ByteArrayMessageElement res = (ByteArrayMessageElement) msg
		.getMessageElement(namespace.toString(), "");
	assert res != null;
	return res;
    }

    /**
         * Extracts the message from msg with the given namespace and type
         * 
         * @param msg
         * @param namespace
         * @param type
         * @return
         */
    protected final static ByteArrayMessageElement getMessageElement(
	    Message msg, MessageType namespace, MessageElementType type) {
	ByteArrayMessageElement res = (ByteArrayMessageElement) msg
		.getMessageElement(namespace.toString(), type.toString());
	assert res != null;
	return res;
    }

    /**
         * A convenience method to add a byte array to a message with the given
         * namespace The element is added with the empty "" tag
         * 
         * @param msg
         * @param namespace
         * @param b
         * @throws IOException
         */
    protected final static void addMessageElement(Message msg,
	    MessageType namespace, byte[] b) {
	addMessageElement("", msg, namespace, b);
    }

    /**
         * Adds a message element of the type elem to the given message (adds
         * such tag to the message element)
         * 
         * @param elem
         * @param msg
         * @param namespace
         * @param b
         * @throws IOException
         */
    protected final static void addMessageElement(MessageElementType elem,
	    Message msg, MessageType namespace, byte[] b) {
	addMessageElement(elem.toString(), msg, namespace, b);
    }

    /**
         * A convenience method to add a byte array to a message with the given
         * namespace The element is added with the given tag
         * 
         * @param msg
         * @param namespace
         * @param b
         * @throws IOException
         */
    protected final static void addMessageElement(String tag, Message msg,
	    MessageType namespace, byte[] b) {
	msg.addMessageElement(namespace.toString(),
		new ByteArrayMessageElement(tag, MimeMediaType.AOS, b, null));
    }

    /**
         * Receives a message that looks like: * And updates the result for the
         * given query.
         * 
         * <pre>
         * Header: |requestId| |tab|
         * Result:
         * (multiple results) |distance| |object|
         * </pre>
         * 
         * @param tab
         * @param id
         * @param msg
         */
    protected abstract void processMatchResult(int tab, long id, Message msg)
	    throws InstantiationException, IllegalAccessException, OBException,
	    DatabaseException;
}
