package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.TimeStampResult;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.BoxNotAvailableException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
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
 * computers. The current implementation uses the JXTA library.
 * @param <O>
 *            The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public abstract class AbstractP2PIndex < O extends OB > implements Index < O >,
        DiscoveryListener, AsynchronousIndex {

    /**
     * Lists all the message types available in the network.
     */
    public static enum MessageType {
        /** time. */
        TIME,
        /** box information (for sync and box selection purposes). */
        BOX,
        /** synchronize box (request for synchronization). */
        SYNCBOX,
        /** synchronize continue (asks for more data). */
        SYNCC,
        /**
         * synchronize retry (asks for the last packet in the event of a
         * timeout).
         */
        SYNCR,
        /** synchronize end (ends the synchronization for one box). */
        SYNCE,
        /** local index data. */
        INDEX,
        /** insert object (after a SYNCBOX). */
        INSOB,
        /** data sync query. */
        DSYNQ,
        /** data sync reply. */
        DSYNR,
        /** search query. */
        SQ,
        /** search result. */
        SR

    };

    /**
     * Lists all the message element types available (these elements are
     * components found in MessageType packets).
     */
    public static enum MessageElementType {
        /** Search header. */
        SH,
        /** Search object. */
        SO,
        /** Search sub result. */
        SSR

    };

    /**
     * Class logger.
     */
    private final transient  Logger logger;

    /**
     * Messages bigger than this one have problems.
     */
    private static final int messageSize = 40 * 1024;

    /**
     * The string that holds the original index xml.
     */
    protected String indexXml;

    /**
     * Min number of pivots to have at any time. For controlling purposes the
     * necessary minimum number of peers to allow matching might be bigger than
     * this.
     */
    public static final int minNumberOfPeers = 2;

    // JXTA variables
    /**
     * JXTA network manager.
     */
    private transient NetworkManager manager;

    /**
     * JXTA discovery service.
     */
    private transient DiscoveryService discovery;

    /**
     * JXTA network client name.
     */
    private String clientName;

    /**
     * JXTA peer group.
     */
    private PeerGroup netPeerGroup;

    /**
     * OBSearch's pipe name.
     */
    private static final String pipeName = "OBSearchPipe";

    /**
     * Maximum number of peers to use.
     */
    private static final  int maxNumberOfPeers = 100;

    /**
     * Interval for each heartbeat (in miliseconds) heartbeats check for missing
     * resources and make sure we are all well connected all the time.
     */
    private static final  int heartBeatInterval = 10000;

    /**
     * General timeout used for most p2p operations.
     */
    private static final  int globalTimeout = 30 * 1000;

    /**
     * Maximum time difference between the peers. peers that have bigger time
     * differences will be dropped.
     */
    private static final  int maxTimeDifference = 3600000;

    /**
     * Maximum number of objects to query at the same time.
     * (60)
     */
    protected static final int maximumItemsToProcess = 15;

    /**
     * Maximum time to wait for a query to be answered.
     */
    protected static final int queryTimeout = 30000;

    /**
     * Internally gives ids that can be used to wait for the
     * processing of a query.
     */
    protected BlockingQueue < Integer > takeATab;

    /**
     * Maximum number of objects to match at the same time. This should be close
     * to the amount of CPUs available. Currently not being used.
     */
    protected int maximumServerThreads;

    /**
     * Object in charge of accepting new connections.
     */
    private JxtaServerPipe serverPipe;

    /**
     * Contains a pipe per client that have tried to connect to us or that We
     * have tried to connect to the key is a peer id and not a pipe!
     */
    private ConcurrentMap < String, PipeHandler > clients;

    /**
     * Time when the index was created.
     */
    protected long indexTime;

    /**
     * Location of this DB. Basically a place were JXTA info is stored.
     */
    private File dbPath;

    /**
     * Object used as a monitor. (hack :))
     */
    private final String timer = "time";

    /**
     * Pipe advertisement. to be re-published every once in a while
     */
    private PipeAdvertisement pipeAdv;

    /**
     * Peer advertisement. to be re-published every once in a while
     */
    private PeerAdvertisement peerAdv;

    /**
     * Lifetime and expiration for advertisements.
     */
    long lifetime = 60 * 2 * 1000L;

    /**
     * Lifetime and expiration for advertisements.
     */
    long expiration = 60 * 2 * 1000L;

    /**
     * The boxes that we are currently serving.
     */
    private int[] ourBoxes;

    /**
     * Total number of boxes that will be supported.
     */
    private int boxesCount;

    /**
     * Each entry has a List. An entry in the array equals to a box #. Every
     * List holds handlers that hold the box in which they are indexed each
     * handler is responsible of registering and unregistering
     */
    private List < List < PipeHandler > > handlersPerBox;

    /**
     * keeps track of the latest handler that was used for each box. at search
     * time this values are used to distribute the queries "evenly"
     */
    private AtomicIntegerArray handlerSearchIndexes;

    /**
     * Set to true when some data is inserted or deleted or when a new peer
     * comes in set to false when we publish all the peers our information or
     * when a new peer comes in.
     */
    private AtomicBoolean boxesUpdated;

    /**
     * If this peer is in client or server mode. If it is in client mode, Peers
     * actively search for other peers. Otherwise the peer just waits for
     * connections. The JXTA library also is configured differently for servers
     * and for clients.
     */
    private boolean isClient = true;

    /**
     * A unique identifier of requests.
     */
    private AtomicLong requestId;

    /**
     * Maximum number of threads to be used in the search.
     */
    private Semaphore searchThreads;

    /**
     * It is set to true when data is inserted or deleted into this index. once
     * the available boxes are published, this variable returns to false.
     */
    private AtomicBoolean modifiedData;

    /**
     * Tells if we are syncing or not. Used to sync only box at a time.
     */
    private AtomicBoolean syncing;

    /**
     * The peer that we are currently syincing.
     */
    private PipeHandler syncingPipe;

    /**
     * The last request time for a box.
     */
    private AtomicLong syncingBoxLastRequestTime;

    /**
     * Initialize the abstract class
     * @param index
     *            the index that will be distributed
     * @param dbPath
     *            the path where we will store information related to this index
     * @param clientName
     *            The name of this peer.
     * @param boxesToServe
     *            The number of boxes that will be served by this index
     * @param maximumServerThreads
     *            Max number of threads to support. (currently it has no effect)
     * @throws IOException
     *             If a serialization exception occurs.
     * @throws PeerGroupException
     *             If a JXTA exception occurs.
     * @throws NotFrozenException
     *             If index is not frozen.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    protected AbstractP2PIndex(final SynchronizableIndex < O > index,
            final File dbPath, final String clientName, final int boxesToServe,
            final int maximumServerThreads) throws IOException,
            PeerGroupException, NotFrozenException, DatabaseException,
            OBException {
        if (!index.isFrozen()) {
            throw new NotFrozenException();
        }
        if (!dbPath.exists()) {
            throw new IOException(dbPath + " does not exit");
        }
        this.maximumServerThreads = maximumServerThreads;
        clients = new ConcurrentHashMap < String, PipeHandler >();
        boxesCount = index.totalBoxes();
        this.dbPath = dbPath;
        this.clientName = clientName;
        this.boxesUpdated = new AtomicBoolean(false);
        logger = Logger.getLogger(clientName);

        handlersPerBox = Collections
                .synchronizedList(new ArrayList < List < PipeHandler >>(index
                        .totalBoxes()));
        initHandlersPerBox(handlersPerBox);

        // initialize the boxes this index is supporting if the given index
        // has the corresponding data.
        if (index.databaseSize() != 0) { // if the database has some
            // data, we serve the data
            // of the db
            int i = 0;
            List < Integer > boxes = new ArrayList < Integer >(index
                    .totalBoxes());
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

        takeATab = new ArrayBlockingQueue < Integer >(maximumItemsToProcess);
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

    /**
     * @return The underlying index
     */
    protected abstract SynchronizableIndex < O > getIndex();

    /**
     * Makes sure that all the components of the given list of lists are
     * synchronized.
     * @param x
     *            A list of lists of pipe handlers.
     */
    private void initHandlersPerBox(final List < List < PipeHandler > > x) {
        int i = 0;
        while (i < boxesCount) {
            x.add(Collections.synchronizedList(new ArrayList < PipeHandler >(
                    maxNumberOfPeers)));
            i++;
        }
    }

    /**
     * Returns true if any box == 0. This means we need sync But only if we are
     * connected to providers of every box.
     * @return True if we have to sync.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private boolean needSync() throws OBException, DatabaseException {
        if (syncing.get()) {
            // we won't sync again if we are syncing already
            // logger.debug("no need to sync because we are syncing");
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
     * @param i
     *            (box #)
     * @return true if I have to sync box i or false otherwise
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private boolean needSyncInBox(final int i) throws OBException,
            DatabaseException {
        long boxTime = getIndex().latestModification(i);

        PipeHandler mostRecent = mostRencentPipeHandlerPerBox(i);
        if (mostRecent == null) {
            return false; // we should wait until we get peers that serve
            // this box
        }
        assert mostRecent.isServing(i);
        long mr = mostRecent.lastUpdated(i);
        // logger.debug(" Most recent box " + mr + " box Time " + boxTime);

        return mr > boxTime;
    }

    /**
     * Returns the pipe handler whose ith box has the most recent modification
     * time.
     * @param i
     *            Box to search
     * @return most recent pipe handler
     */
    private PipeHandler mostRencentPipeHandlerPerBox(final int i) {
        List < PipeHandler > boxList = handlersPerBox.get(i);
        PipeHandler ph = null;
        synchronized (boxList) {
            long time = -1;

            Iterator < PipeHandler > it = boxList.iterator();
            while (it.hasNext()) {
                PipeHandler p = it.next();
                assert p.isServing(i);
                if (time < p.lastUpdated(i)) {
                    time = p.lastUpdated(i);
                    ph = p;
                }
            }
        }
        return ph;
    }

    /**
     * Initializes the p2p network.
     * @param isClient
     *            If this index is a client peer.
     * @param c
     *            Configuration mode
     * @param clearCache
     *            If the cache has to be cleared
     * @param seedURI
     *            The url of the seed file.
     * @throws IOException
     *             If some network error occurs.
     * @throws PeerGroupException
     *             If some JXTA error occurs.
     */
    private void init(final boolean isClient,
            final NetworkManager.ConfigMode c, final boolean clearCache,
            final URI seedURI) throws IOException, PeerGroupException {

        File cache = new File(new File(dbPath, ".cache"), clientName);
        if (clearCache) {
            Directory.deleteDirectory(cache);
        }
        manager = new NetworkManager(c, clientName, cache.toURI());
        NetworkConfigurator configurator = manager.getConfigurator();

        // clear the seeds
        configurator.setRendezvousSeedURIs(new LinkedList < String >());
        configurator.setRelaySeedURIs(new LinkedList < String >());
        configurator.setRelaySeedingURIs(new HashSet < String >());
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
        pipeAdv = getPipeAdvertisement();
        // init the incoming connection listener
        serverPipe = new JxtaServerPipe(netPeerGroup, pipeAdv);
        serverPipe.setPipeTimeout(0);

        peerAdv = netPeerGroup.getPeerAdvertisement();

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

        // publishAdvertisements();

        assert netPeerGroup.getPeerAdvertisement().equals(
                netPeerGroup.getPeerAdvertisement());
    }

    /**
     * Publish advertisements
     * @throws IOException
     *             if JXTA signals an error.
     */
    private void publishAdvertisements() throws IOException {
        discovery.publish(peerAdv, lifetime, expiration);
        discovery.remotePublish(peerAdv, expiration);
        discovery.publish(pipeAdv, expiration, expiration);
        discovery.remotePublish(pipeAdv, expiration);
    }

    /**
     * This class performs a heartbeat. It makes sure that all the resources we
     * need to properly work.
     */
    private class HeartBeat implements Runnable {

        /**
         * If we caught an error.
         */
        private boolean error = false;

        /**
         * This method starts network connections and calls heartbeat
         * undefinitely until the program stops.
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
                } catch (IOException e) {
                    // a pipe gave some error, this is expected
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

        /**
         * Executed once per heart beat.
         * @throws IOException
         *             If some network error occurs.
         * @throws PeerGroupException
         *             If some JXTA error occurs.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         */
        public void heartBeat1() throws PeerGroupException, IOException,
                OBException, DatabaseException {
            if (needSync()) {
                // lower
                sync();
            }

        }

        /**
         * Executed every 10 heart beats.
         * @param count
         *            The current heart beat count.
         * @throws IOException
         *             If some network error occurs.
         * @throws PeerGroupException
         *             If some JXTA error occurs.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         */
        public void heartBeat10(final long count) throws DatabaseException,
                IOException, OBException, PeerGroupException {
            if (count % 10 == 0) {
                // find pipes if not enough peers are available
                // or if not all the boxes have been covered
                if (!minimumNumberOfPeers() || !totalBoxesCovered()) {
                    // logger.debug("Finding pipes!");
                    findPipes();
                }
                // check timeouts
                //
            }
        }

        /**
         * Executed every 6 heart beats.
         * @param count
         *            The current heart beat count.
         * @throws IOException
         *             If some network error occurs.
         * @throws PeerGroupException
         *             If some JXTA error occurs.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         */
        public void heartBeat6(final long count) throws PeerGroupException,
                IOException, OBException, DatabaseException {
            if (count % 6 == 0) {
               
            }
        }

        /**
         * Executed every 3 heart beats.
         * @param count
         *            The current heart beat count.
         * @throws IOException
         *             If some network error occurs.
         * @throws PeerGroupException
         *             If some JXTA error occurs.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         */
        public void heartBeat3(final long count) throws PeerGroupException,
                IOException, OBException, DatabaseException {
            if (count % 3 == 0) {

                sendBoxInfo();
                info();

                // check for timeouts in the sync process
                syncAlive();
                publishAdvertisements();
                queryTimeoutCheck();
            }
        }

        /**
         * Executed every 100 heart beats.
         * @param count
         *            The current heart beat count.
         * @throws IOException
         *             If some network error occurs.
         * @throws PeerGroupException
         *             If some JXTA error occurs.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         */
        public void heartBeat100(final long count) throws PeerGroupException,
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
     * @return true if the index is still processing query results
     */
    public final boolean isProcessingQueries() {
        return takeATab.size() != maximumItemsToProcess;
    }

    /**
     * Monitors all the queries being executed.
     */
    protected final void queryTimeoutCheck() {
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
     * Process the query entry for tab "tab"
     * @param tab
     *            The tab to be processed.
     * @param time
     *            The current time.
     */
    protected abstract void queryTimeoutCheckEntry(int tab, long time);

    /**
     * Prerequisites: Each PipeHandler contains information of the latest
     * modification of each of its served boxes. The sync method has to
     * accomplish several things: 1) Compare the latest updates performed by
     * other pipes and if there is someone with a more recent update, ask for
     * the data 2) Decide if we shall serve other boxes depending on the desired
     * amount of boxes to serve and the number of boxes currently served by the
     * peers that surround us.
     * @throws IOException
     *             If some network error occurs.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
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
     * Prints some information of the peer.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private void info() throws OBException, DatabaseException {
        int[] servicedBoxes = servicedBoxes();
        if (servicedBoxes != null) {
            int[] boxCount = new int[servicedBoxes.length];
            // long[] times = new long[servicedBoxes.length];
            int i = 0;
            while (i < boxCount.length) {
                boxCount[i] = getIndex().elementsPerBox(i);

                /*
                 * if(logger.isDebugEnabled()){ times[i] =
                 * getIndex().latestModification(i); }
                 */

                i++;
            }
            logger.info("Heart: Connected Peers: " + clients.size() + " B: "
                    + Arrays.toString(ourBoxes) + ", boxes: "
                    + Arrays.toString(boxCount));
            /*
             * if(logger.isDebugEnabled()){ logger.debug("Latest modifications:" +
             * Arrays.toString(times)); }
             */
        } else {
            logger.info("Heart: Connected Peers: " + clients.size()
                    + " no boxes being served. ");
        }
    }

    /**
     * Make sure the sync process is alive. If it is not, retry the sync.
     * @throws IOException
     *             If some network error occurs.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
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
     * Receives the # of elements per box, and: If we are not serving any box,
     * it finds this.minBoxesToServe boxes. It will select boxes whose count is
     * the least. If we are serving boxes, and we are serving a box that is
     * exceedingly being served, then
     * @param boxesToServe #
     *            of boxes that will be served
     * @param index
     *            The index used to find out the total amount of boxes available
     *            Changes the internal ourBoxes array with the boxes that will
     *            be served by this index
     */
    private void decideServicedBoxes(final int boxesToServe,
            final SynchronizableIndex < O > index) {
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

    /**
     * Returns true if x is found in arr in the interval [0,i[.
     * @param x
     *            the x to search
     * @param i
     *            the maximum bound
     * @param arr
     *            the array
     * @return True if x is found in the interval [0,i[
     */
    private boolean inArray(final int x, final int i, final int[] arr) {
        int cx = 0;
        while (cx < i) {
            if (arr[cx] == x) {
                return true;
            }
            cx++;
        }
        return false;
    }

    /**
     * For each pipe in pipes, send a time message.
     * @throws IOException
     *             If some network error occurs.
     */
    private void timeBeat() throws IOException {
        synchronized (clients) {
            Iterator < PipeHandler > it = this.clients.values().iterator();
            while (it.hasNext()) {
                PipeHandler u = it.next();
                u.sendTimeMessage();
            }
        }
    }

    /**
     * Sends box information to all the peers if box information has been
     * changed.
     * @throws IOException
     *             If some network error occurs.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private void sendBoxInfo() throws IOException, DatabaseException,
            OBException {

        synchronized (clients) {
            Iterator < PipeHandler > it = clients.values().iterator();
            while (it.hasNext()) {
                PipeHandler u = it.next();
                u.sendBoxMessage();
            }
        }

    }

    /**
     * @return true if we are connected to the minimum number of peers.
     */
    protected final boolean minimumNumberOfPeers() {
        return this.clients.size() >= AbstractP2PIndex.minNumberOfPeers;
    }

    /**
     * @return The number of peers connected to this peer.
     */
    public final int getNumberOfPeers() {
        return this.clients.size();
    }

    /**
     * Returns true if all the peers have data that is synchronized to the same
     * timestamp. This should not be used normally but it is useful for testing
     * purposes.
     * @return true if all the peers are in sync with me. *
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    // TODO: Improve this so that boxes are only sent when we modify one
    // of our boxes
    public final boolean areAllPeersSynchronizedWithMe() throws OBException,
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
            List < PipeHandler > b = handlersPerBox.get(i);
            Iterator < PipeHandler > it = b.iterator();
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
     * Check if we have peers who serve at least one box per box #.
     * @return true if the above condition applies
     */
    public final boolean areAllBoxesAvailable() {

        Iterator < List < PipeHandler >> it = handlersPerBox.iterator();
        while (it.hasNext()) {
            List < PipeHandler > l = it.next();
            if (l.size() == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method must be called by all users once It starts the network, and
     * creates some background threads like the hearbeat and the incoming
     * connection handler.
     * @param client
     *            If true, the index will be created in client mode (from the
     *            p2p network perspective) If false, the index will be a
     *            "server".
     * @param clearPeerCache
     *            If we should clear network related cache information
     * @param seedFile
     *            The seed file to be used for this index. Only the given seeds
     *            will be used
     * @throws IOException
     *             If some network error occurs.
     * @throws PeerGroupException
     *             If some network error occurs.
     */
    public final void open(final boolean client, final boolean clearPeerCache,
            final File seedFile) throws IOException, PeerGroupException {
        this.isClient = client;
        NetworkManager.ConfigMode c = null;
        if (!seedFile.exists()) {
            throw new IOException("File does not exist: " + seedFile);
        }
        if (client) {
            c = NetworkManager.ConfigMode.RELAY;
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

    /**
     * Generates a pipe advertisement.
     * @return A pipe advertisement.
     */
    private PipeAdvertisement getPipeAdvertisement() {
        PipeID pipeID = (PipeID) ID.create(generatePipeID());

        PipeAdvertisement advertisement = (PipeAdvertisement) AdvertisementFactory
                .newAdvertisement(PipeAdvertisement.getAdvertisementType());

        advertisement.setPipeID(pipeID);
        advertisement.setType(PipeService.UnicastType);
        advertisement.setName(pipeName);
        return advertisement;
    }

    /**
     * Returns the pipe ID in URI form.
     * @return an URI representing the pipe id.
     */
    private URI generatePipeID() {
        return IDFactory.newPipeID(netPeerGroup.getPeerGroupID()).toURI();
    }

    /**
     * Finds the pipe for the given peer. Gets advertisements of pipes for the
     * given peer.n
     * @param peer
     *            The peer we are going to search.
     * @throws IOException
     *             If some network error occurs.
     * @throws PeerGroupException
     *             If some network error occurs.
     */
    protected final void findPipePeer(final URI peer) throws IOException,
            PeerGroupException {
        if (!minimumNumberOfPeers()) {
            discovery.getRemoteAdvertisements(peer.toString(),
                    DiscoveryService.ADV, "Name", pipeName, 1, null);

        }
    }

    /**
     * Query the discovery service for OBSearch pipes.
     * @throws IOException
     *             If some network error occurs.
     * @throws PeerGroupException
     *             If some network error occurs.
     */
    protected final void findPipes() throws IOException, PeerGroupException {
        if (isClient) {
            discovery.getRemoteAdvertisements(null, DiscoveryService.PEER,
                    null, null, minNumberOfPeers, null);
        }

    }

    /**
     * Check if all the boxes are being supplied.
     * @return true if all the boxes are represented by a peer.
     */
    protected final boolean totalBoxesCovered() {
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
     * Method called by the discovery server.
     * @param ev
     *            The event that the discovery server found.
     */
    public final void discoveryEvent(final DiscoveryEvent ev) {

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
     * @param id
     *            The id of the peer we are looking for
     * @return True if the given id is connected to us.
     */
    private boolean isConnectedToPeer(final String id) {
        return clients.containsKey(id);
    }

    /**
     * Adds the given pipe to our cache of pipes. The pipe is added if our quote
     * of pipes is under the minimum. and if we don't have the pipe already
     * @param id
     *            peerId of the given peer advertisement
     * @param p
     *            peer advertisement to be added
     */

    private void addPipe(final URI id, final PipeAdvertisement p) {

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
     * Adds the given pipe. This is called either when an accept is made by the
     * server, and when the discovery returns from the server.
     * @param id
     *            (peer id of the given bidipipe)
     * @param ph
     *            pipe handler to add.
     * @throws IOException
     *             If some network error occurs.
     * @throws Exception
     *             If something really bad happens.
     */
    private void addPipeAux(final URI id, final PipeHandler ph)
            throws IOException, Exception {
        synchronized (clients) {
            if (isConnectedToPeer(id.toString())) {

                /*
                 * try { logger .debug("Closing because we are already connected
                 * to it: " + id); ph.close(); } catch (IOException e) { logger
                 * .fatal("Error while trying to close a duplicated pipe" + e);
                 * assert false; }
                 */

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
     * Add the given bidipipe
     * @param bidipipe
     *            Bidipipe to add.
     * @throws IOException
     *             If some network error occurs.
     * @throws Exception
     *             If something really bad happens.
     */
    private void addPipe(final PipeHandler bidipipe) throws IOException,
            Exception {
        synchronized (clients) {
            URI id = bidipipe.getPeerID();
            addPipeAux(id, bidipipe);
        }
    }

    /**
     * Returns the currently boxes served by the library.
     * @return an int array with the serviced boxes.
     * @throws OBException
     */
    private int[] servicedBoxes() {
        return this.ourBoxes;
    }

    /**
     * Closes the given pipe. all resources are released.
     * @param id
     *            the pipe id that will be closed.
     * @throws IOException
     *             If some network error occurs.
     */
    private void closePipe(final String id) throws IOException {
        synchronized (clients) {
            PipeHandler pipe = clients.get(id);
            clients.remove(id);
            pipe.close();
        }
    }

    /**
     * This class holds all the methods required to communicate with another
     * peer.
     */
    protected class PipeHandler implements PipeMsgListener {

        /**
         * The communication channel used to talk to the peer at the other end.
         */
        private JxtaBiDiPipe pipe;

        /**
         * The biggest time difference found between us and the peer at the
         * other end.
         */
        private long biggestTimeDifference = Long.MIN_VALUE;

        /**
         * The peer id at the other end.
         */
        private URI peerID;

        /**
         * This holds the last time the boxes of the underlying pipe were
         * updated. The peer is responsible of telling us when he changed. value
         * of 0 means that the box is not being served
         */
        private AtomicLongArray boxLastUpdated;

        /**
         * We keep track of the latest sent timestamp in order to avoid
         * re-sending data. YAY! (minimizes duplicated data transmission
         * considerably)
         */
        private AtomicLong[] lastSentTimestamp;

        /**
         * We keep track of the most recent SYNCBOX request after a SYNCBOX
         * request several SYNCC (SYNC Continue) messages will be sent until we
         * send the receiver a SYNCE (SYNC End) message.
         */
        private TimeStampIterator insertIterator = null;

        /**
         * latest sync msg is stored here, in the event of a timeout msg from
         * the peer at the other end, we just resend the bytes stored here.
         */
        private TupleOutput syncRetry;

        /**
         * Constructor for the pipe handler.
         */
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
         * Creates a new pipe handler.
         * @param id
         *            The original peer id
         * @param pipe
         *            The pipe we will
         */
        public PipeHandler(final URI id, final JxtaBiDiPipe pipe) {
            this();
            synchronized (pipe) {
                this.pipe = pipe;
                this.peerID = id;
                pipe.setMessageListener(this);
            }
        }

        /**
         * Creates a new pipe handler.
         * @param p
         *            PipeAdvertisement to use to create this pipe handler.
         */
        public PipeHandler(final PipeAdvertisement p) throws IOException {
            this();

            pipe = new JxtaBiDiPipe();

            // pipe.setReliable(true);
            
                pipe.connect(netPeerGroup, null, p, globalTimeout, this);
                peerID = pipe.getRemotePeerAdvertisement().getPeerID().toURI();
        }

        /**
         * @return The peer id that is at the other end
         */
        public URI getPeerID() {
            return peerID;
        }

        /**
         * Sends a sync request to the underlying pipe. The pipe will
         * asyncrhonally respond with all the objects that he has ed or deleted
         * from "time" and only for the given box Does not send the message if
         * the given time is
         * @param boxIndex
         *            the index of the box in the ourBoxes array
         * @param time
         *            Time of the sync message.
         * @throws BoxNotAvailableException
         *             If the box given is out of range.
         * @throws IOException
         *             If some network error occurs.
         */
        public void sendRequestSyncMessage(final int boxIndex, final long time)
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
         * latest_modification (long) |_n Where: box #: box identification
         * number object_count: the number of objects in the given box
         * latest_modification: the most recent modification time. n: total
         * number of servied boxes.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IOException
         *             if some network error occurs.
         */
        // TODO: This method
        // * should only be invoked when: 1) We just connected with the peer 2)
        // * our data has been modified
        public void sendBoxMessage() throws DatabaseException, OBException,
                IOException {

            SynchronizableIndex < O > index = getIndex();
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
         * @throws IOException
         *             if some network error occurs.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
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

        /**
         * Creates a time message.
         * @return a new time message with the current time.
         * @throws IOException
         *             if some network error occurs.
         */
        private Message makeTimeMessage() throws IOException {
            return makeTimeMessageAux(System.currentTimeMillis());
        }

        /**
         * Sends a time message to the underlying pipe.
         * @throws IOException
         *             if some network error occurs.
         */
        public final void sendTimeMessage() throws IOException {
            // time sync message
            sendMessage(makeTimeMessage());
        }

        /**
         * Auxiliary method for {@link #makeTimeMessage()}.
         * @param time
         *            the time to use to create the time message
         * @return A new time message
         * @throws IOException
         *             if some network error occurs.
         */
        private Message makeTimeMessageAux(final long time) throws IOException {
            TupleOutput out = new TupleOutput();
            out.writeLong(time);
            Message msg = new Message();
            addMessageElement(msg, MessageType.TIME, out.getBufferBytes());
            return msg;
        }

        /**
         * Parses the time embedded in a time message and returns the time.
         * @param msg
         *            A message
         * @return The time from the message.
         */
        private long parseTimeMessage(final Message msg) {
            ByteArrayMessageElement m = getMessageElement(msg, MessageType.TIME);
            TupleInput in = new TupleInput(m.getBytes());
            return in.readLong();
        }

        /**
         * This method is called when a message comes into our pipe.
         * @param event
         *            Event sent by JXTA
         */
        public final void pipeMsgEvent(final PipeMsgEvent event) {

            Message msg = event.getMessage();
            if (msg == null) {
                return;
            }
            // ************************************************
            // We handle here all the different messages we will receive.
            // ************************************************
            try {
                Iterator < String > it = msg.getMessageNamespaces();
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

        /**
         * Retry the sync process.
         * @throws IOException
         *             If some network error occurs.
         * @throws OBException
         *             User generated exception
         * @throws DatabaseException
         *             If something goes wrong with the DB
         */
        private void processSyncRetry() throws IOException, OBException,
                DatabaseException {
            logger.debug("Doing re-sync");
            sendInsertMessageFromTuple(syncRetry);
        }

        /**
         * Reset sync info.
         */
        private void resetSyncInfo() {
            syncing.set(false);
            syncingPipe = null;
            syncingBoxLastRequestTime.set(-1);
        }

        /**
         * Re-sends a sync message (after a timeout).
         * @throws IOException
         *             If some network error occurs.
         */
        public void sendReSyncMessage() throws IOException {
            Message msg = new Message();
            // logger.debug("Give me more data!");
            addMessageElement(msg, MessageType.SYNCR, new byte[1]);
            sendMessage(msg);
            updateSyncInfo();
        }

        /**
         * Update internal sync information.
         */
        private void updateSyncInfo() {
            syncing.set(true);
            syncingPipe = this;
            syncingBoxLastRequestTime.set(System.currentTimeMillis());
        }

        /**
         * Process the sync-end messsage.
         * @throws OBException
         *             User generated exception
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws IOException
         *             if something goes wrong with the network
         */
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
         *            the message to be processed
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IllegalAccessException
         *             If there is a problem when instantiating objects O
         * @throws InstantiationException
         *             If there is a problem when instantiating objects O
         */
        private void processSearchResponse(final Message msg)
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
         * @param msg
         *            Message where the query comes.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IllegalAccessException
         *             If there is a problem when instantiating objects O
         * @throws InstantiationException
         *             If there is a problem when instantiating objects O
         */
        private void processSearchQuery(final Message msg) throws IOException,
                InstantiationException, IllegalAccessException, OBException,
                DatabaseException {
            // searchThreads.acquireUninterruptibly();
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
         * @param msg
         *            Message where the insert data comes.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IllegalAccessException
         *             If there is a problem when instantiating objects O
         * @throws InstantiationException
         *             If there is a problem when instantiating objects O
         */
        private void processInsertOB(final Message msg)
                throws InstantiationException, IllegalAccessException,
                OBException, DatabaseException, IOException {
            ElementIterator it = msg
                    .getMessageElementsOfNamespace(MessageType.INSOB.toString());
            int i = 0;
            long time = -2;
            // logger.debug("Starting insertion");
            long t = System.currentTimeMillis();
            int repeatedItems = 0;
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
                        boolean insert = in.readBoolean();
                        O o = readObject(in);
                        // logger.info("Inserting object");
                        Result res = new Result(Status.NOT_EXISTS);
                        if (insert) {
                            res = getIndex().insert(o, time);
                        } else {
                            getIndex().delete(o, time);
                        }
                        if (res.getStatus() != Status.OK) {
                            repeatedItems++;
                        }
                        // update the sync info so that timeouts won't occurr
                        updateSyncInfo();
                        cx++;
                    }
                    logger.debug("Inserted objects: " + cx + " in "
                            + (System.currentTimeMillis() - t)
                            + " msec. Repeated:" + repeatedItems);
                } catch (IndexOutOfBoundsException e) {
                    // we are done
                }
                i++;
            }
            assert i == 1;
            boxesUpdated.set(true);

            sendSyncCMsg();
        }

        /**
         * Send sync continue message. After receiving sync updates we re-send
         * the info again to continue synchronization.
         * @throws IOException
         *             if something goes wrong with the network
         */
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
         * @param msg
         *            Message where thhe sync request info comes.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IOException
         *             If a network error occurs.
         */
        private void processSyncBox(final Message msg) throws OBException,
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
            insertIterator = (TimeStampIterator) getIndex().elementsNewerThan(
                    box, time);

            sendNextSyncMessage();
        }

        /**
         * Continues sending sync messages.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IOException
         *             If a network error occurs.
         */
        private void sendNextSyncMessage() throws IOException,
                DatabaseException, OBException {
            // we will write here all the bytes
            if (insertIterator == null) {
                // return because we have to wait for an "official"
                // sync request based on something
                sendEndSyncMessage();
                return;
            }
            synchronized (insertIterator) {
                TupleOutput out = new TupleOutput();
                int i = 0;
                while (insertIterator.hasNext()) {
                    TimeStampResult < O > r = insertIterator.next();
                    O o = r.getObject();
                    // format: <time> <data>
                    long t = r.getTimestamp();
                    out.writeLong(t);
                    out.writeBoolean(r.isInsert());
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

        /**
         * Sends the final sync message.
         * @throws IOException
         *             If a network error occurs.
         */
        private void sendEndSyncMessage() throws IOException {
            Message msg = new Message();
            addMessageElement(msg, MessageType.SYNCE, new byte[1]);
            sendMessage(msg);
        }

        /**
         * Sends the given tupleoutput as an insert message.
         * @param out
         *            Byte stream to send.
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IOException
         *             If a network error occurs.
         */
        private void sendInsertMessageFromTuple(final TupleOutput out)
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
         * heart the decision of getting data from the underlying Peer.
         * @param msg
         *            Receive the message containing the box info data.
         */
        private void processBox(final Message msg) {
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
         * Returns the latest updated information for the given box.
         * @param box
         * @return latest updated information for the given box
         */
        public long lastUpdated(final int box) {
            assert box >= 0 && box <= boxesCount : "Box inputted:" + box;
            return this.boxLastUpdated.get(box);
        }

        /**
         * Returns true if the given box is being served by this peer.
         * @param box
         *            The box that will be queried.
         * @return true if the given box is served by this peer.
         */
        public boolean isServing(final int box) {
            return this.boxLastUpdated.get(box) != 0;
        }

        /**
         * Sends a message to the underlying pipe.
         * @param msg
         *            the message to send
         * @throws IOException
         *             If a network error occurs.
         */
        public void sendMessage(final Message msg) throws IOException {
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
         * @param msg
         *            Message with the time element.
         * @throws IOException
         *             If a network error occurs.
         */
        private void processTime(final Message msg) throws IOException {
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
         * Closes the underlying pipe and releases resources.
         * @throws IOException
         *             If a network error occurs.
         */
        public void close() throws IOException {
            if (pipe != null) {
                synchronized (pipe) {
                    synchronized (handlersPerBox) {
                        pipe.close();
                        pipe = null;
                        removeFromHandler();
                        this.insertIterator.close();
                    }
                }
            }
        }

        /**
         * Removes from the handlers the given this PipeHandler.
         */
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

        /**
         * Constructor.
         */
        public IncomingConnectionHandler() {

        }

        /**
         * Continously listens for new peers.
         */
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

    /**
     * Closes the index. Releases JXTA resources.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     */
    public void close() throws DatabaseException, OBException {
        manager.stopNetwork();
        getIndex().close();
    }

    /**
     * Returns the database size of the underlying index.
     * @return The database size.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     */
    public int databaseSize() throws DatabaseException, OBStorageException {
        return getIndex().databaseSize();
    }

    /**
     * Deletes the given object form the database. The changes will be
     * incrementally propagated to all the connected peers.
     * @param object
     *            The object to be deleted
     * @return >= (the object ID) if the object was deleted or -1 if the object
     *         was not deleted
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @since 0.0
     */
    public Result delete(final O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        Result res = getIndex().delete(object);

        this.boxesUpdated.set(true);
        return res;
    }

    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException
             {
        getIndex().freeze();

    }

    public int getBox(final O object) throws OBException {
        return getIndex().getBox(object);
    }

    public O getObject(final int i) throws DatabaseException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException {
        return getIndex().getObject(i);
    }

    /**
     * Inserts the given object into the index. If the index is not frozen, you
     * are expected to insert objects that are not duplicated OBSearch cannnot
     * efficiently perform those checks for you at this stage (before freezing),
     * so please be careful. The changes will be incrementally propagated to all
     * the connected peers.
     * @param object
     *            The object to be added Identification number of the given
     *            object. This number must be responsibly generated by someone
     * @return The internal id of the object(>= 0) or -1 if the object exists in
     *         the database
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @since 0.0
     */
    public Result insert(final O object) throws IllegalIdException,
            DatabaseException, OBException, IllegalAccessException,
            InstantiationException {

        Result res = getIndex().insert(object);
        boxesUpdated.set(true);
        return res;
    }

    public boolean isFrozen() {
        return getIndex().isFrozen();
    }

    public int totalBoxes() {
        return getIndex().totalBoxes();
    }

    public void relocateInitialize(final File dbPath) throws DatabaseException,
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

    public O readObject(final TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        return getIndex().readObject(in);
    }

    public Result exists(final O object) throws DatabaseException,
            OBException, IllegalAccessException, InstantiationException {
        return getIndex().exists(object);
    }

    /**
     * This class holds information used to process each query
     */
    protected abstract class QueryProcessing {
        /**
         * last request performed.
         */
        protected long lastRequestId;

        /**
         * Last request timestamp (to perform timeouts).
         */
        protected long lastRequestTime;

        /**
         * Contains the remaining boxes (to be processed)
         */
        protected int[] remainingBoxes;

        /**
         * Holds the index that points the next boxes that have to be explored.
         */
        protected int boxIndex;

        /**
         * Contains the previous index just in case we have to re-send the
         * request.
         */
        protected int prevIndex;

        /**
         * tab obtained, implicit in the array but we keep because it is
         * pysicologically more comfortable to have it (for me AJMM)
         */
        protected int tab;

        /**
         * The object that is being matched.
         */
        protected O object;

        /**
         * @return The object being matched.
         */
        public O getObject() {
            return object;
        }

        /**
         * Sets the object being matched
         * @param object
         *            The object to be set.
         */
        public void setObject(final O object) {
            this.object = object;
        }

        /**
         * Constructor.
         * @param remainingBoxes
         *            Remaining boxes to match
         * @param tab
         *            The obtained tab
         * @param object
         *            The object to be matched
         */
        public QueryProcessing(final int[] remainingBoxes, final int tab,
                final O object) {
            super();
            this.remainingBoxes = remainingBoxes;
            boxIndex = 0;
            lastRequestId = -1;
            lastRequestTime = -1;
            this.tab = tab;
            this.object = object;
        }

        /**
         * Return current box index.
         * @return (box that we are matching)
         */
        public int getBoxIndex() {
            return boxIndex;
        }

        /**
         * Set box index.
         * @param boxIndex
         *            (box that we are matching)
         */
        public void setBoxIndex(final int boxIndex) {
            this.boxIndex = boxIndex;
        }

        /**
         * Return the last request id. Every time we query one peer, a request
         * id is created.
         * @return The request id.
         */
        public long getLastRequestId() {
            return lastRequestId;
        }

        /**
         * Set last request id
         * @param lastRequestId
         *            (current request id)
         */
        public void setLastRequestId(final long lastRequestId) {
            this.lastRequestId = lastRequestId;
        }

        /**
         * Get the last time we performed a request
         * @return The last request time.
         */
        public long getLastRequestTime() {
            return lastRequestTime;
        }

        /**
         * Sets the last time we performed a request
         * @param The
         *            last request time.
         */
        public void setLastRequestTime(final long lastRequestTime) {
            this.lastRequestTime = lastRequestTime;
        }

        /**
         * @return the list of boxes that have to be matched.
         */
        public int[] getRemainingBoxes() {
            return remainingBoxes;
        }

        /**
         * Set the list of boxes that have to be matched
         * @param remainingBoxes
         */
        public void setRemainingBoxes(final int[] remainingBoxes) {
            this.remainingBoxes = remainingBoxes;
        }

        /**
         * Prev index is the index of the previously sent box... for retry
         * purposes *
         * @return the prevIndex
         */
        public int getPrevIndex() {
            return prevIndex;
        }

        /**
         * Prev index is the index of the previously sent box... for retry
         * purposes *
         * @param the
         *            prev index
         */
        public void setPrevIndex(final int prevIndex) {
            this.prevIndex = prevIndex;
        }

        /**
         * @return the tab for this query request
         */
        public int getTab() {
            return tab;
        }

        /**
         * Sets the tab
         * @param tab
         */
        public void setTab(final int tab) {
            this.tab = tab;
        }

        /**
         * @return true if the matching is done.
         */
        protected boolean isFinished() {
            return this.remainingBoxes.length >= boxIndex;
        }

        /**
         * Updates the remainingBoxes array. Removes the boxes already processed
         * @param newBoxes
         *            (this array is destroyed by this method)
         */
        protected void updateRemainingBoxes(final int[] newBoxes) {
            List < Integer > res = new LinkedList < Integer >();
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
            Iterator < Integer > it = res.iterator();
            while (it.hasNext()) {
                remainingBoxes[i] = it.next();
                i++;
            }
        }

        /**
         * Returns true if x is in array. We search array for indexes <
         * max_index
         * @param x
         * @param array
         * @param max_index
         * @return true if x is in array
         */
        private boolean exists(final int x, final int[] array,
                final int max_index) {
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
         * @param msg
         *            The result message *
         * @throws DatabaseException
         *             If something goes wrong with the DB
         * @throws OBException
         *             User generated exception
         * @throws IllegalAccessException
         *             If there is a problem when instantiating objects O
         * @throws InstantiationException
         *             If there is a problem when instantiating objects O
         */
        public abstract void handleResult(Message msg)
                throws InstantiationException, IllegalAccessException,
                OBException, DatabaseException;

        /**
         * Balances the peers used to make queries.
         * Finds the next peer that will match the box
         * that we have to match.
         * @return a peer that will match our next query.
         */
        protected PipeHandler findNextPipeHandler() {
            int nextBox = remainingBoxes[boxIndex];
            List < PipeHandler > hl = handlersPerBox.get(nextBox);
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
         * @param ph PipeHandler that will be queried
         * @return A list of boxes that will be searched in this pipe handler.
         */
        protected List < Integer > findBoxesToSearch(final PipeHandler ph) {
            // get the boxes this pipe handler is serving and determine
            // which boxes will be matched in the given pipe handler
            // based on the current boxes to match.
            int i = boxIndex;
            List < Integer > res = new LinkedList < Integer >();
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

        /**
         * @return True if the range has changed for this query
         */
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
                List < Integer > boxesToSearch = findBoxesToSearch(ph);

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
                List < Integer > boxesToSearch = findBoxesToSearch(ph);
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
         * Creates a new query message. Uses the current state of the object. Header:
         * long (lastRequestId)| int (box #) | int (box id)...
         * @param boxesToSearch Boxes that will be queried.
         */
        protected Message createCurrentQueryMessage(
                final List < Integer > boxesToSearch) {
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
            Iterator < Integer > it = boxesToSearch.iterator();
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
         * @param out Byte stream where the k will be written.
         */
        protected abstract void writeK(TupleOutput out);

        /**
         * Writes down the current range employed in the search
         * @param out Byte stream where the range will be written
         */
        protected abstract void writeRange(TupleOutput out);

        /**
         * Writes the currently found objects and their respective ranges to the
         * message
         * @param msg msg where to add the information
         */
        protected abstract void addResult(Message msg);
    }

    /**
     * This method must obtain the object to be matched and other parameters
     * from the given message and search the underlying index. The returning
     * message is ready to be transmitted to the peer that requested the query.
     * @param boxes boxes that will be matched.
     * @param msg Message that contains other query data besides boxes
     * @return A new message (result) ready to be sent to the other end.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    protected abstract Message performMatch(int[] boxes, Message msg)
            throws InstantiationException, IllegalAccessException, OBException,
            DatabaseException;

    /**
     * Extracts the message associated to the given namespace. Assumes that the
     * message only contains one element
     * @param msg The message to be extracted
     * @param namespace The namespace to extract.
     * @return The ByteArrayMessageElement associated to the only MessageElement
     *         in this Message
     */
    public static final  ByteArrayMessageElement getMessageElement(
            final Message msg, final MessageType namespace) {
        ByteArrayMessageElement res = (ByteArrayMessageElement) msg
                .getMessageElement(namespace.toString(), "");
        assert res != null;
        return res;
    }

    /**
     * Extracts the message from msg with the given namespace and type
     * @param msg Message that will be parsed
     * @param namespace Namespace that will be extracted
     * @param type Type (Message Element name) that will be extracted
     * @return The Message element.
     */
    protected final static ByteArrayMessageElement getMessageElement(
            final Message msg, final MessageType namespace,
            final MessageElementType type) {
        ByteArrayMessageElement res = (ByteArrayMessageElement) msg
                .getMessageElement(namespace.toString(), type.toString());
        assert res != null;
        return res;
    }

    /**
     * A convenience method to add a byte array to a message with the given
     * namespace. The element is added with the empty "" tag
     * @param msg Message where we will add the bytes
     * @param namespace Namespace to use
     * @param b Byte array that will be employed.
     */
    protected final static void addMessageElement(final Message msg,
            final MessageType namespace, final byte[] b) {
        addMessageElement("", msg, namespace, b);
    }

    /**
     * Adds a message element of the type elem to the given message (adds such
     * tag to the message element)
     * @param elem Element type to add
     * @param msg Add the bytes to this message
     * @param namespace And use this namespace
     * @param b Bytes to add
     */
    protected final static void addMessageElement(
            final MessageElementType elem, final Message msg,
            final MessageType namespace, final byte[] b) {
        addMessageElement(elem.toString(), msg, namespace, b);
    }

    /**
     * A convenience method to add a byte array to a message with the given
     * namespace. The element is added with the given tag
     * @param tag The tag that will be added
     * @param msg Message where we will add the bytes
     * @param namespace Namespace to use
     * @param b Bytes to add
     */
    protected final static void addMessageElement(final String tag,
            final Message msg, final MessageType namespace, final byte[] b) {
        msg.addMessageElement(namespace.toString(),
                new ByteArrayMessageElement(tag, MimeMediaType.AOS, b, null));
    }

    /**
     * Receives a message with objects. Updates the result for the
     * given query. Will process a match result. If more matches
     * have to be performed, this method will query again the peers.
     * The message received looks like:
     * <pre>
     * Header: |requestId| |tab|
     * Result:
     * (multiple results) |distance| |object|
     * </pre>
     * 
     * @param tab Tab of the result
     * @param id Id of the result
     * @param msg Message with additional information.
     *     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    protected abstract void processMatchResult(int tab, long id, Message msg)
            throws InstantiationException, IllegalAccessException, OBException,
            DatabaseException;
}
