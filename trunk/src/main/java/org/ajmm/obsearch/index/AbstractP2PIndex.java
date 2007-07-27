package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.document.MimeMediaType;
import net.jxta.endpoint.ByteArrayMessageElement;
import net.jxta.endpoint.Message;
import net.jxta.endpoint.MessageElement;
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

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.BoxNotAvailableException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
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
		DiscoveryListener {

	public static enum MessageType {
		TIME, // time
		BOX, // box information (for sync and box selection purposes)
		SYNCBOX, // synchronize box
		INDEX, // local index data
		DSYNQ, // data sync query
		DSYNR, // data sync reply
		SQ, // search query
		SR
		// search response
	};

	private transient final Logger logger;

	// the string that holds the original index xml
	protected String indexXml;

	// min number of pivots to have at any time... for controlling purposes
	// the necessary minimum number of peers to allow matching might be bigger
	// than this.
	protected final int minNumberOfPeers = 5;

	// JXTA variables
	private transient NetworkManager manager;

	private transient DiscoveryService discovery;

	private String clientName;

	private final static String pipeName = "OBSearchPipe";

	private final static int maxAdvertisementsToFind = 5;

	private final static int maxNumberOfPeers = 100;
	
	// extra time used in operations that are related to
	// time syncrhonizations. It is like a cushion to reduce the possibility that
	// records are not copied properly
	private final static long timeFill = 60000;

	// Interval for each heartbeat (in miliseconds)
	// heartbeats check for missing resources and make sure we are all well
	// connected all the time
	private final static int heartBeatInterval = 10000;

	// general timeout used for most p2p operations
	private final static int globalTimeout = 60000;

	// maximum time difference between the peers.
	// peers that have bigger time differences will be dropped.
	private final static int maxTimeDifference = 3600000;

	private JxtaServerPipe serverPipe;

	// contains a pipe per client that have tried to connect to us or that we
	// have tried to connect to
	// the key is a peer id and not a pipe!
	private ConcurrentMap<URI, PipeHandler> clients;

	// time when the index was created
	protected long indexTime;

	// holds the boxes that this index is currently supporting
	private BitSet availableBoxes;

	// contains all the pipes separated by box type
	// only if our index is in full mode
	private Queue[] searchPipes;

	private PeerGroup netPeerGroup;

	private File dbPath;

	private String timer = "time";

	private PipeAdvertisement adv;

	protected abstract SynchronizableIndex<O> getIndex();
	
	// The boxes that we are currently serving
	private int [] ourBoxes;
	
	// the minimum amount of boxes that will be served
	private int minBoxesToServe;
	
	// Each entry has a List. An entry in the array equals to a box #.
	// Every List holds handlers that hold the box in which they are indexed
	// each handler is responsible of registering and unregistering
	private List<ArrayList<PipeHandler>> handlersPerBox; 

	/**
	 * Initialize the abstract class
	 * 
	 * @param index
	 *            the index that will be distributed
	 * @param dbPath
	 *            the path where we will store information related to this index
	 *            @param boxesToServe The number of boxes that will be served by this index
	 * @throws IOException
	 * @throws PeerGroupException
	 * @throws NotFrozenException
	 */
	protected AbstractP2PIndex(SynchronizableIndex<O> index, File dbPath,
			String clientName, int boxesToServe) throws IOException, PeerGroupException,
			NotFrozenException {
		if (!index.isFrozen()) {
			throw new NotFrozenException();
		}
		if (!dbPath.exists()) {
			throw new IOException(dbPath + " does not exist");
		}
		clients = new ConcurrentHashMap<URI, PipeHandler>();
		searchPipes = new Queue[index.totalBoxes()];
		availableBoxes = new BitSet(index.totalBoxes());
		this.dbPath = dbPath;
		this.clientName = clientName;
		this.minBoxesToServe = boxesToServe;
		logger = Logger.getLogger(clientName);
		
		handlersPerBox = Collections.synchronizedList(new ArrayList<ArrayList<PipeHandler>>(index.totalBoxes()));
		initHandlersPerBox(handlersPerBox);
	}
	
	// makes sure that all the components of the given list of lists are synchronized
	private void initHandlersPerBox(List<ArrayList<PipeHandler>> x){
		int i = 0;
		while(i < x.size()){
			x.set(i, (ArrayList<PipeHandler>) Collections.synchronizedList(new ArrayList<PipeHandler>(maxNumberOfPeers)));
			i++;
		}
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

		discovery.publish(adv);
		discovery.publish(netPeerGroup.getPeerAdvertisement());
		discovery.remotePublish(netPeerGroup.getPeerAdvertisement());
		
		// wait for rendevouz connection
		if (isClient) {
			logger.debug("Waiting for rendevouz connection");
			manager.waitForRendezvousConnection(0);
			logger.debug("Rendevouz connection found");
		}				

	}

	/**
	 * This class performs a heartbeat. It makes sure that all the resources we
	 * need to properly work.
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
		public void heartBeat1() throws PeerGroupException, IOException {			
			timeBeat();
		}

		// executed once every 3 heart beats
		public void heartBeat3(long count) throws PeerGroupException,
				IOException {
			if (count % 3 == 0) {
				// send my time to everybody, telling them what is my current
				// time
				// if someone doesn't like my time, they will desconnect from
				// me.
				findPipes();
				logger.info("heart: " + clients.size());
			}
		}

		// executed once every 100 heartbeats
		public void heartBeat100(long count) throws Exception {
			if (count % 100 == 0) {
				// advertisements should be proactively searched for if we are
				// running out
				// of connections
				sync();
			}
		}
	}
	
	/**
	 * Prerequisites:
	 * Each PipeHandler contains information of the latest modification of each
	 * of its served boxes.
	 * The sync method has to accomplish several things:
	 * 1) Compare the latest updates performed by other pipes and if there is 
	 *     someone with a more recent update, ask for the data
	 *  2) Decide if we shall serve other boxes depending on the desired amount of boxes to serve
	 *      and the number of boxes currently served by the peers that surround us.
	 */
	private void sync() throws OBException, DatabaseException, IOException{
		
		SynchronizableIndex<O> index = getIndex();
		// get the latest modification per box 
		// and leave in maxPh the pipe that holds this latest modification
		Iterator<PipeHandler> it = this.clients.values().iterator();
		int max = getIndex().totalBoxes();
		long [] latestModifications = new long[max];
		int [] boxCount = new int [max];
		int i = 0;
		// init the array with the latest modification time
		while(i < max){
			latestModifications[i] = index.latestModification(i);
			i++;
		}
		ArrayList<PipeHandler> maxPh = new ArrayList<PipeHandler>(max);
		while(it.hasNext()){
			PipeHandler ph = it.next();
			i = 0;
			while(i < max){
				if(ph.isServing(i)){
					if(latestModifications[i] < ph.lastUpdated(i)){
						latestModifications[i] = ph.lastUpdated(i);
						maxPh.set(i, ph);
					}
					boxCount[i]++;
				}
				i++;
			}
		}
		// determine what boxes are we going to serve from now on
		decideServicedBoxes(boxCount);
		// ask for a sync to the latest peer of the data we need
		int [] servicedBoxes = this.servicedBoxes();
		i = 0;
		while(i < servicedBoxes.length){
			int box = servicedBoxes[i];
			PipeHandler ph = maxPh.get(box);
			if(ph != null){
				try{
					ph.sendRequestSyncMessage(box, index.latestModification(box));
				}catch(BoxNotAvailableException e){
					// the pipe handler changed while we were 
				}
			}
			i++;
		}
	}
	
	/**
	 * Receives the # of elements per box, and:
	 * If we are not serving any box, it finds this.minBoxesToServe
	 * boxes. It will select boxes whose count is the least.
	 * If we are serving boxes, and we are serving a box that is
	 * exceedingly being served, then 
	 * @param boxCount
	 */
	private synchronized void decideServicedBoxes(int [] boxCount) throws Exception{
		int [] sb = this.servicedBoxes();
		int findBoxes = 1; // number of boxes to find
		if(sb == null){
			findBoxes = minBoxesToServe;
			sb = new int[minBoxesToServe];
		}
		int i  = 0;
		while(i < findBoxes){
			// find one box that should be replaced
			int badBox = findWorstBox(boxCount);
			if(sb != null){
				// find the box that should not be served
				// returns the index that should be removed
				// this cannot leave any box in 0.
				// if it can't be avoided then we signal an exception
				int myGoodBox = findBestBox(sb, boxCount);
				int toRemoveBox = sb[myGoodBox];
				sb[myGoodBox] = badBox;
				// updates boxCount to reflect the new layout
				boxCount[toRemoveBox]--;
				boxCount[badBox]++;
			}
			i++;
		}
		this.ourBoxes = sb;
	}

	/**
	 * For each pipe in pipes, send a time message
	 * 
	 */
	private void timeBeat() throws IOException {
		Iterator<PipeHandler> it = this.clients.values().iterator();
		while (it.hasNext()) {
			PipeHandler u = it.next();
			u.sendTimeMessage();
		}
	}
	
	/**
	 * Extracts the message associated to the given namespace Assumes that the
	 * message only contains one element
	 * 
	 * @param msg
	 * @param namespace
	 * @return The ByteArrayMessageElement associated to the only MessageElement
	 *         in this Message
	 */
	protected final ByteArrayMessageElement getMessageElement(Message msg,
			MessageType namespace) {
		ByteArrayMessageElement res = (ByteArrayMessageElement) msg.getMessageElement(namespace
				.toString(), "");
		assert res != null;
		return res;
	}

	

	/**
	 * Send the given message to the pipeID
	 * 
	 * @param pipeID
	 * @param msg
	 * @throws IOException
	 */
	protected final void sendMessage(URI pipeID, Message msg)
			throws IOException {
		PipeHandler p = clients.get(pipeID);
		if (p != null) {
			p.sendMessage(msg);
		}else{
			assert false;
		}
	}

	/**
	 * For every pipe we have registered, we send them the globalBoxCount so
	 * that everybody has an overall idea on how many boxes are being served
	 * currently. We only need to call this method when: 1) We have changed the
	 * set of boxes we are serving 2) someone is telling us that they have done
	 * the same, and their information is more recent than ours.
	 */
	protected void syncGlobalBoxesInformation() {

	}

	protected boolean minimumNumberOfPeers() {
		return this.clients.size() >= this.minNumberOfPeers;
	}

	/**
	 * This method must be called by all users once It starts the network, and
	 * creates some background threads like the hearbeat and the incoming
	 * connection handler
	 * 
	 * @param client
	 *            If true, the index will be created in client mode (from the
	 *            p2p network perspective) If false, the index will be a
	 *            "server".
	 * @param clearPeerCache
	 *            If we should clear network related cache information
	 * @param seedFile
	 *            The seed file to be used for this index. Only the given seeds
	 *            will be used
	 */
	public void open(boolean client, boolean clearPeerCache, File seedFile)
			throws IOException, PeerGroupException {
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
		if (logger.isDebugEnabled()) {
			logger.debug("Generated pipeid: " + pipeID);
		}
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

	private boolean readyToAcceptConnections() {
		assert false;
		return false;
	}

	/** Finds the pipe for the given peer * */
	protected void findPipePeer(URI peer) throws IOException,
			PeerGroupException {
		if (!minimumNumberOfPeers()) {
			discovery.getRemoteAdvertisements(peer.toString(),
					DiscoveryService.ADV, "Name", pipeName, 1, this);
		}
	}

	// query the discovery service for OBSearch pipes
	protected void findPipes() throws IOException, PeerGroupException {

		if (!minimumNumberOfPeers()) {
			// logger.debug("Getting advertisements");
			discovery.getRemoteAdvertisements(null, DiscoveryService.PEER,
					null, null, 0, this);
		}
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
						PipeAdvertisement p = (PipeAdvertisement) adv;
						addPipe(p);
					} else if (adv instanceof PeerAdvertisement) {
						// if we get a peer advertisement, then if the peer is
						// not yet connected to us
						// we can search for pipes.
						PeerAdvertisement p = (PeerAdvertisement) adv;
						if (!clients.containsKey(p.getPeerID().toURI())) {
							// find pipes
							findPipePeer(p.getPeerID().toURI());
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
	 * Adds the given pipe to our cache of pipes. The pipe is added if our quote
	 * of pipes is under the minimum. TODO: Remove synchronized ?
	 * 
	 * @param peerID
	 *            peerId of the given peer advertisement
	 * @param p
	 *            peer advertisement to be added
	 */
	private synchronized void addPipe(PipeAdvertisement p) {

		try {
			// only if we don't have already the connection
			JxtaBiDiPipe pipe = new JxtaBiDiPipe();
			PipeHandler ph = new PipeHandler(p);
			addPipeAux(ph.getPeerID(),ph);

		} catch (IOException e) {
			logger.fatal("IO Error while trying to add Pipe:" + p + " \n ", e);
			assert false;
		} catch(Exception e){
			logger.fatal("Error while trying to add Pipe:" + p + " \n ", e);
			assert false;
		}

	}

	/**
	 * Adds the given pipe. This is called either when an accept is made by the
	 * server, and when the discovery returns from the server.
	 * 
	 * @param id
	 *            (peer id of the given bidipipe)
	 * @param ph
	 * @throws IOException
	 */
	private synchronized void addPipeAux(URI id, PipeHandler ph)
			throws IOException, Exception {
		if (clients.containsKey(id)) {
			try {
				ph.close();
			} catch (IOException e) {
				logger.fatal("Error while trying to close a duplicated pipe"
						+ e);
				assert false;
			}
		} else if (! clients.containsKey(id) && clients.size() <= maxNumberOfPeers) {
			logger.debug("Adding pipe of peer: " + ph.getPeerID());
			this.clients.put(id, ph);
			// send initial sync data to make sure that everybody is
			// syncrhonized enough to be meaningful.
			ph.sendMessagesAfterFirstEncounter();
		} else {
			try {
				ph.close();
			} catch (IOException e) {
				logger.fatal("Error while closing pipe" + e);
				assert false;
			}
		}
	}

	/**
	 * TODO: Remove synchronized ?
	 * 
	 * @param bidipipe
	 */
	private synchronized void addPipe(PipeHandler bidipipe) throws IOException, Exception {
		URI id = bidipipe.getPeerID();
		if (!clients.containsKey(id)) {
			addPipeAux(id, bidipipe);
		}
	}
	
	/**
	 * Returns the currently boxes served by the library
	 * @return
	 * @throws OBException
	 */
	private int [] servicedBoxes() throws OBException, DatabaseException{
		return this.ourBoxes;
	}

	/**
	 * Closes the given pipe. all resources are released.
	 */
	private void closePipe(URI id) throws IOException {
		PipeHandler pipe = clients.get(id);
		pipe.close();
	}

	
	private class PipeHandler implements  PipeMsgListener{
		
		private JxtaBiDiPipe pipe;

		// the biggest time difference found 
		private long biggestTimeDifference = Long.MIN_VALUE;
		
		private URI peerID;
		
		// This holds the last time the boxes of the underlying pipe
		// were updated. The peer is responsible of telling us when 
		// he changed.
		// value of 0 means that the box is not being served	
		private AtomicLongArray boxLastUpdated;

		public PipeHandler(){
			boxLastUpdated = new AtomicLongArray(getIndex().totalBoxes());
		}
		/**
		 * Creates a new pipe handler
		 * @param id The original peer id
		 * @param pipe The pipe we will 
		 */
		public PipeHandler(URI id, JxtaBiDiPipe pipe) {
			this();
			this.pipe = pipe;			
			this.peerID = id;
			pipe.setMessageListener(this);
		}
		
		public PipeHandler(PipeAdvertisement p) throws IOException{
			this();
			pipe = new JxtaBiDiPipe();
			pipe.connect(netPeerGroup, null, p, globalTimeout, this);
			peerID = pipe.getRemotePeerAdvertisement().getPeerID().toURI();
		}
		
		public URI getPeerID(){
			return peerID;
		}
		
		/**
		 * Sends a sync request to the underlying pipe.
		 * The pipe will asyncrhonally respond with all the objects that he has inserted or deleted
		 * from "time" and only for the given box
		 * @param box
		 * @param time
		 * @throws BoxNotAvailableException
		 * @throws IOException
		 */
		public void sendRequestSyncMessage(int box, long time) throws BoxNotAvailableException, IOException{
			if(boxLastUpdated.get(box) == 0){
				throw new BoxNotAvailableException();
			}
			TupleOutput out = new TupleOutput();
			out.writeInt(box);
			// calculate the time in the other server
			long x =  timeFill;
			if( 0 > biggestTimeDifference){
				x =x * -1;
			}
			long mukouTime = time + (biggestTimeDifference  + x );
			out.writeLong(time);
			Message msg = new Message();
			addMessageElement(msg, MessageType.SYNCBOX, out.getBufferBytes());
			sendMessage(msg);			
		}
		
		/**
		 * Sends to the underlying pipe, the following data:
		 * | n (int) |
		 * | box # (int) | latest_modification (long) |_1
		 * | box # (int) | latest_modification (long) |_2
		 * ...
		 * | box # (int) | latest_modification (long) |_n
		 * 
		 * Where:
		 *  box #: box identification number
		 *  object_count: the number of objects in the given box
		 *  latest_modification: the most recent modification time.
		 *  n: total number of servied boxes
		 *  This method should only be invoked when:
		 *  1) We just connected with the peer
		 *  2) our data has been modified
		 */
		public void sendBoxMessage() throws DatabaseException, OBException, IOException{
			SynchronizableIndex<O> index = getIndex();
			int [] serviced = servicedBoxes();
			int i = 0;
			TupleOutput out = new TupleOutput();
			out.writeInt(serviced.length);
			while(i < serviced.length){
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
		 * When the peers encounter each other for the first time, we send a set of
		 * standard sync messages only sent to the given bidipipe. This is to make
		 * sure that from the beginning we have performed all the standard sync
		 * steps. The same syncs will be performed at various frequencies by the
		 * heart.
		 * 
		 * @param bidipipe
		 *            The pipe that to which we will send messages.
		 */
		public void sendMessagesAfterFirstEncounter()
				throws IOException, DatabaseException, OBException {
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
		 * @throws IOException
		 */
		public final void sendTimeMessage() throws IOException{
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
		
		/**
		 * A convenience method to add a byte array to a message with the given
		 * namespace The element is added with the empty "" tag
		 * 
		 * @param msg
		 * @param namespace
		 * @param b
		 * @throws IOException
		 */
		private final void addMessageElement(Message msg, MessageType namespace,
				byte[] b) throws IOException {
			msg.addMessageElement(namespace.toString(),
					new ByteArrayMessageElement("", MimeMediaType.AOS, b, null));
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
			// ************************************************
			// We handle here all the different messages we will receive.
			// ************************************************
			try {
				Iterator<String> it = msg.getMessageNamespaces();
				while (it.hasNext()) {
					String namespace = it.next();
					try {
						MessageType messageType = MessageType.valueOf(namespace);

						switch (messageType) {
						case TIME:
							processTime(msg);
							break;
						case BOX:
							processBox(msg);
							break;
						default:
							assert false;
						}
					} catch (IllegalArgumentException i) {
						// we got a message that is not ours
					}

				}
			} catch (IOException io) {
				logger.fatal("Exception while receiving message", io);
				assert false;
			}
		}
		
		// Process the given msg that should contain a BOX element
		// we leave our heart the decision of getting data from the underlying Peer.
		// TODO: this method is inneficient on purpose. We want to have it as
		// correct as possible. We don't expect this method to be called a lot,
		// so it should ok for now.
		private synchronized void processBox(Message msg){
			ByteArrayMessageElement m = getMessageElement(msg, MessageType.BOX);
			assert m != null; 
			TupleInput in = new TupleInput(m.getBytes());
			int totalBoxes = in.readInt();
			int i = 0;
			// initialize the array with zeros.
			while(i < boxLastUpdated.length()){
				boxLastUpdated.set(i, 0);
				// shifts all the elements to the 
				handlersPerBox.get(i).remove(this); 
				i++;
			}
			i = 0;
			// update the boxes that were sent in the message
			while(i < totalBoxes){
				int box = in.readInt();
				long time = in.readLong();
				this.boxLastUpdated.set(box, time);
				handlersPerBox.get(box).add(this);
				i++;
			}			
		}
		
		
		/**
		 * Returns the latest updated information for the given box
		 * @param box
		 * @return latest updated information for the given box
		 */
		public long lastUpdated(int box){
			return this.boxLastUpdated.get(box);
		}
		
		/**
		 * Returns true if the given box is being served by this peer
		 * @param box
		 * @return
		 */
		public boolean isServing(int box){
			return this.boxLastUpdated.get(box) != 0;
		}
		
		
		/**
		 * Sends a message to the underlying pipe
		 * @param msg
		 * @throws IOException
		 */
		public void sendMessage(Message msg) throws IOException{
			this.pipe.sendMessage(msg);
		}
		
		/**
		 * Recevies a message that contains a time MessageElement
		 * 
		 * @param msg
		 * @param peerID
		 *            (the pipe we received the
		 */
		private void processTime(Message msg) throws IOException {
			long time = parseTimeMessage(msg);
			long ourtime = System.currentTimeMillis();
			long diff = time - ourtime;
			if (logger.isDebugEnabled()) {
				logger.debug("Received time " + time + " from peer: " + peerID
						+ " diff: " + diff);
			}
			// time must be within +- k units away from our current time.
			// otherwise we drop the connection with the given pipe id
			if (Math.abs(diff) > maxTimeDifference) {
				logger.debug("Rejected " + time + " from peer: " + peerID);
				closePipe(peerID);
			}
			if(Math.abs(biggestTimeDifference) < Math.abs(diff)){
				this.biggestTimeDifference = diff;
			}
		}
		
		/**
		 * Closes the underlying pipe and releases results
		 */
		public void close() throws IOException{
			pipe.close();
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
					// add the pipe to our list of pipes, if we can hold more
					// people.
					URI id = bidipipe.getRemotePeerAdvertisement().getPeerID().toURI();
					PipeHandler ph = new PipeHandler(id, bidipipe);
					addPipe(ph);
				} catch (IOException e) {
					logger.fatal("Error while listening to a connection:" + e);
					assert false;

				}catch(Exception e){
					logger.fatal("Error while listening to a connection:" + e);
					assert false;
				}
			}
		}

	}

	public void close() throws DatabaseException {
		manager.stopNetwork();
		getIndex().close();
	}

	public int databaseSize() throws DatabaseException {
		return getIndex().databaseSize();
	}

	public int delete(O object) throws NotFrozenException, DatabaseException {
		return getIndex().delete(object);
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
		return getIndex().insert(object);
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

}