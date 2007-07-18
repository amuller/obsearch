package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import net.jxta.discovery.DiscoveryEvent;
import net.jxta.discovery.DiscoveryListener;
import net.jxta.discovery.DiscoveryService;
import net.jxta.document.Advertisement;
import net.jxta.document.AdvertisementFactory;
import net.jxta.endpoint.Message;
import net.jxta.exception.PeerGroupException;
import net.jxta.id.ID;
import net.jxta.peergroup.PeerGroup;
import net.jxta.pipe.PipeID;
import net.jxta.pipe.PipeMsgEvent;
import net.jxta.pipe.PipeMsgListener;
import net.jxta.pipe.PipeService;
import net.jxta.platform.NetworkManager;
import net.jxta.protocol.DiscoveryResponseMsg;
import net.jxta.protocol.PipeAdvertisement;
import net.jxta.util.JxtaBiDiPipe;
import net.jxta.util.JxtaServerPipe;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.apache.log4j.Logger;

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

public abstract class AbstractP2PIndex implements Index, DiscoveryListener,
		PipeMsgListener {

	private static transient final Logger logger = Logger
			.getLogger(AbstractP2PIndex.class);

	// time when the index was created
	protected long indexTime;

	// the string that holds the original index xml
	protected String indexXml;

	// JXTA variables
	private transient NetworkManager manager;

	private transient DiscoveryService discovery;

	private final static String clientName = "OBSearchClient";

	private final static String pipeName = "OBSearchPipe";

	private final static int maxAdvertisementsToFind = 5;

	private final static NetworkManager.ConfigMode ConfigMode = NetworkManager.ConfigMode.ADHOC;

	private final static int maxNumberOfPeers = 100;

	// general timeout used for most p2p operations
	private final static int globalTimeout = 60000;

	private JxtaServerPipe serverPipe;

	private ConcurrentMap<URI, JxtaBiDiPipe> pipes = new ConcurrentHashMap<URI, JxtaBiDiPipe>();

	protected AbstractP2PIndex(File path) throws IOException,
			PeerGroupException {
		init();
	}

	private void init() throws IOException, PeerGroupException {
		manager = new NetworkManager(ConfigMode, clientName, new File(new File(
				".cache"), clientName).toURI());
		manager.startNetwork();
		// Get the NetPeerGroup
		PeerGroup netPeerGroup = manager.getNetPeerGroup();

		// get the discovery service
		discovery = netPeerGroup.getDiscoveryService();
		discovery.addDiscoveryListener(this);

		// init the incoming connection listener

		serverPipe = new JxtaServerPipe(manager.getNetPeerGroup(),
				getPipeAdvertisement());

	}

	private PipeAdvertisement getPipeAdvertisement() {
		PipeID pipeID = (PipeID) ID.create(URI.create(generatePipeID()));
		PipeAdvertisement advertisement = (PipeAdvertisement) AdvertisementFactory
				.newAdvertisement(PipeAdvertisement.getAdvertisementType());

		advertisement.setPipeID(pipeID);
		advertisement.setType(PipeService.UnicastType);
		advertisement.setName(pipeName);
		return advertisement;
	}

	private String generatePipeID() {
		assert false;
		return "";
	}

	private boolean readyToAcceptConnections() {
		assert false;
		return false;
	}

	// query the discovery service for OBSearch pipes
	protected void findAdvertisements() throws IOException, PeerGroupException {

		discovery.getRemoteAdvertisements(null, DiscoveryService.ADV, "Name",
				pipeName, maxAdvertisementsToFind, null);
	}

	/**
	 * Method called by the discovery server, when we discover something
	 */
	public void discoveryEvent(DiscoveryEvent ev) {

		DiscoveryResponseMsg res = ev.getResponse();
		Advertisement adv;
		Enumeration en = res.getAdvertisements();

		if (en != null) {
			while (en.hasMoreElements()) {
				adv = (Advertisement) en.nextElement();
				if (adv instanceof PipeAdvertisement) {
					PipeAdvertisement p = (PipeAdvertisement) adv;
					addPipe(p);
				}
			}
		}
	}

	/**
	 * Adds the given pipe to our cache of pipes. The pipe is added if our quote
	 * of pipes is under the minimum.
	 * TODO: Remove synchronized ?
	 * @param p
	 */
	private synchronized  void addPipe(PipeAdvertisement p) {

		try {
			if (pipes.size() <= maxNumberOfPeers) {
				// only if we don't have already the connection
				if(! this.pipes.containsKey(p.getPipeID().toURI())){
					JxtaBiDiPipe pipe = new JxtaBiDiPipe();
					pipe.connect(manager.getNetPeerGroup(), null, p, globalTimeout,
						this);
					pipe.connect(manager.getNetPeerGroup(), p);
					// 
					addPipeAux(pipe);
				}
			}
		} catch (IOException e) {
			logger.fatal("Error while trying to add Pipe:" + p + " \n " + e);
			assert false;
		}

	}
	
	private synchronized void addPipeAux(JxtaBiDiPipe bidipipe){
		if(pipes.containsKey(bidipipe.getPipeAdvertisement().getPipeID().toURI())){
			try{ // a duplicated pipe, we should close the new one and leave the old connection open
				bidipipe.close();
			}catch(IOException e){
				logger.fatal("Error while trying to close a duplicated pipe" + e);
				assert false;
			}
		}else{
			this.pipes.put(bidipipe.getPipeAdvertisement().getPipeID().toURI() , bidipipe);
		}
	}
	
	/**
	 * Obtains the PipeAdvertisement of the given pipe and
	 * if we can hold more peers, we add it to our list of peers
	 * TODO: Remove synchronized ?
	 * @param bidipipe
	 */
	private synchronized void addPipe(JxtaBiDiPipe bidipipe){
		if (pipes.size() <= maxNumberOfPeers) {
			addPipeAux(bidipipe);
		}else{
			try{
				bidipipe.close();
			}catch(IOException e){				
				logger.fatal("Error while closing pipe" + e);
				assert false;
			}
		}
	}

	/**
	 * This method is called when a message comes into our pipe.
	 */
	public void pipeMsgEvent(PipeMsgEvent event) {
		Message msg;
		msg = event.getMessage();
		// *****************************************************
		// *****************************************************
		// we can now handle all the different messages we will receive.
		// *****************************************************

	}

	private class IncomingConnectionHandler implements Runnable {

		public IncomingConnectionHandler() {

		}

		public void run() {

			System.out
					.println("Waiting for JxtaBidiPipe connections on JxtaServerPipe");
			while (true) {
				try {
					JxtaBiDiPipe bidipipe = serverPipe.accept();
					// add the pipe to our list of pipes, if we can hold more people.
					addPipe(bidipipe);
				} catch (IOException e) {
					assert false;
					logger.fatal("Error while listening to a connection:" + e);
				}
			}
		}

	}

	public void close() throws DatabaseException {

	}

	public int databaseSize() throws DatabaseException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int delete(OB object) throws NotFrozenException, DatabaseException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			DatabaseException, OutOfRangeException, OBException,
			UndefinedPivotsException {
		// TODO Auto-generated method stub

	}

	public int getBox(OB object) throws OBException {
		// TODO Auto-generated method stub
		return 0;
	}

	public OB getObject(int i) throws DatabaseException, IllegalIdException,
			IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return null;
	}

	public int insert(OB object) throws IllegalIdException, DatabaseException,
			OBException, IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isFrozen() {
		// TODO Auto-generated method stub
		return false;
	}

	public int totalBoxes() {
		// TODO Auto-generated method stub
		return 0;
	}

}
