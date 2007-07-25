package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
		DiscoveryListener, PipeMsgListener {

	public static enum MessageType {
		TIME, // time
		GBOX, // global boxes
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
	private ConcurrentMap<URI, JxtaBiDiPipe> clients;

	// time when the index was created
	protected long indexTime;

	// holds the boxes that this index is currently supporting
	private BitSet availableBoxes;

	// contains all the pipes separated by box type
	// only if our index is in full mode
	private Queue[] searchPipes;

	// This is used to decide which boxes we will service
	// total number of serviced boxes
	// we update them direcly if we decide to change some boxes.
	// From time to time, we send the update to everybody who is connected to
	// us.
	// we also receive this from everybody and we update accordigly
	private int[] globalBoxCount;

	private long[] globalBoxLastUpdated;

	private PeerGroup netPeerGroup;

	private File dbPath;

	private String timer = "time";

	private PipeAdvertisement adv;

	protected abstract SynchronizableIndex<O> getIndex();

	/**
	 * Initialize the abstract class
	 * 
	 * @param index
	 *            the index that will be distributed
	 * @param dbPath
	 *            the path where we will store information related to this index
	 * @throws IOException
	 * @throws PeerGroupException
	 * @throws NotFrozenException
	 */
	protected AbstractP2PIndex(SynchronizableIndex<O> index, File dbPath,
			String clientName) throws IOException, PeerGroupException,
			NotFrozenException {
		if (!index.isFrozen()) {
			throw new NotFrozenException();
		}
		if (!dbPath.exists()) {
			throw new IOException(dbPath + " does not exist");
		}
		clients = new ConcurrentHashMap<URI, JxtaBiDiPipe>();
		searchPipes = new Queue[index.totalBoxes()];
		availableBoxes = new BitSet(index.totalBoxes());
		this.dbPath = dbPath;
		this.clientName = clientName;

		logger = Logger.getLogger(clientName);
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
		configurator.setRendezvousSeeds(new HashSet());
		configurator.setRelaySeedingURIs(new HashSet());
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

		this.netPeerGroup.getDiscoveryService().publish(adv);
		this.netPeerGroup.getDiscoveryService().remotePublish(netPeerGroup.getPeerAdvertisement());
		
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
		public void heartBeat100(long count) throws PeerGroupException,
				IOException {
			if (count % 100 == 0) {
				// advertisements should be proactively searched for if we are
				// running out
				// of connections

			}
		}
	}

	/**
	 * For each pipe in pipes, send a time message
	 * 
	 */
	private void timeBeat() throws IOException {
		Iterator<URI> it = this.clients.keySet().iterator();
		while (it.hasNext()) {
			URI u = it.next();
			sendMessage(u, makeTimeMessage());
		}
	}

	private final Message makeTimeMessage() throws IOException {
		return makeTimeMessageAux(System.currentTimeMillis());
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
		return (ByteArrayMessageElement) msg.getMessageElement(namespace
				.toString(), "");
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

	/**
	 * Send the given message to the pipeID
	 * 
	 * @param pipeID
	 * @param msg
	 * @throws IOException
	 */
	protected final void sendMessage(URI pipeID, Message msg)
			throws IOException {
		JxtaBiDiPipe p = clients.get(pipeID);
		if (p != null) {
			p.sendMessage(msg);
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

		// finding strategy:
		// find only new peers.
		// when we get a peer in the discovery event, we call for its pipe only
		// after
		// we made sure that the peer is unknown.
		/*
		 * discovery.getRemoteAdvertisements( // no specific peer (propagate)
		 * null, //Adv type DiscoveryService.ADV, //Attribute = any null,
		 * //Value = any null, // one advertisement response is all we are
		 * looking for 1, // no query specific listener. we are using a global
		 * listener null); synchronized(discovery){ try{ discovery.wait(5000);
		 * }catch(InterruptedException e){ e.printStackTrace(); } }
		 */

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
	 * @param id
	 *            peerId of the given peer advertisement
	 * @param p
	 *            peer advertisement to be added
	 */
	private synchronized void addPipe(PipeAdvertisement p) {

		try {
			// only if we don't have already the connection
			JxtaBiDiPipe pipe = new JxtaBiDiPipe();
			logger.debug("Connect!");
			pipe.connect(netPeerGroup, null, p, globalTimeout, this);
			URI id = pipe.getRemotePeerAdvertisement().getPeerID().toURI();
			addPipeAux(id, pipe);

		} catch (IOException e) {
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
	 * @param bidipipe
	 * @throws IOException
	 */
	private synchronized void addPipeAux(URI id, JxtaBiDiPipe bidipipe)
			throws IOException {
		if (clients.containsKey(id)) {
			try {
				bidipipe.close();
			} catch (IOException e) {
				logger.fatal("Error while trying to close a duplicated pipe"
						+ e);
				assert false;
			}
		} else if (! clients.containsKey(id) && clients.size() <= maxNumberOfPeers) {
			logger.debug("Adding pipe of peer: " + bidipipe.getRemotePeerAdvertisement().getID());
			this.clients.put(id, bidipipe);
			// send initial sync data to make sure that everybody is
			// syncrhonized enough to be meaningful.
			sendMessagesAfterFirstEncounter(bidipipe);
		} else {
			try {
				bidipipe.close();
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
	private synchronized void addPipe(JxtaBiDiPipe bidipipe) throws IOException {
		URI id = bidipipe.getRemotePeerAdvertisement().getPeerID().toURI();
		if (!clients.containsKey(id)) {
			bidipipe.setMessageListener(this);
			addPipeAux(id, bidipipe);
		}
	}

	/**
	 * This method is called when a message comes into our pipe.
	 */
	public void pipeMsgEvent(PipeMsgEvent event) {

		Message msg = event.getMessage();
		URI pipeId = event.getPipeID().toURI();

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
						processTime(msg, pipeId);
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

	/**
	 * Recevies a message that contains a time MessageElement
	 * 
	 * @param msg
	 * @param id
	 *            (the pipe we received the
	 */
	private void processTime(Message msg, URI id) throws IOException {
		long time = parseTimeMessage(msg);
		long ourtime = System.currentTimeMillis();
		if (logger.isDebugEnabled()) {
			logger.debug("Received time " + time + " from peer: " + id
					+ " diff: " + (time - ourtime));
		}
		// time must be within +- k units away from our current time.
		// otherwise we drop the connection with the given pipe id
		if (Math.abs(time - ourtime) > maxTimeDifference) {
			logger.debug("Rejected " + time + " from peer: " + id);
			closePipe(id);
		}
	}

	/**
	 * Closes the given pipe. all resources are released.
	 */
	private void closePipe(URI id) throws IOException {
		JxtaBiDiPipe pipe = clients.get(id);
		pipe.close();
	}

	/*private class PipeHandler implements Runnable {
		
		private JxtaBiDiPipe pipe;

		// variables used to control the time difference between
		// this computer's time and the peer's computer time
		private long timeCounts;

		private long totalTime;

		public PipeHandler(JxtaBiDiPipe pipe) {
			this.pipe = pipe;
		}
	}*/

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
					addPipe(bidipipe);
				} catch (IOException e) {
					logger.fatal("Error while listening to a connection:" + e);
					assert false;

				}
			}
		}

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
	private void sendMessagesAfterFirstEncounter(JxtaBiDiPipe bidipipe)
			throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("Sending messages after first encounter to "
					+ bidipipe.getPipeAdvertisement().getPipeID().toURI());
		}
		// time sync message
		sendMessage(bidipipe.getPipeAdvertisement().getPipeID().toURI(),
				makeTimeMessage());
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
