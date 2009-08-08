package net.obsearch.index.knngraph;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;

import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;
import net.obsearch.utils.bytes.ByteConversion;

/**
 * Since neo does not allow to define ids, then we have to control two types of
 * ids: 1) neo ids 2) OBSearch ids.
 * 
 * The ub-tree will hold ids of type 1). Each node in the graph will hold ids of
 * type 2.
 * 
 * @author Arnoldo Jose Muller-Molina
 * 
 * @param <O>
 * @param <B>
 * @param <Q>
 * @param <BC>
 */
public abstract class AbstractKnnGraph<O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractBucketIndex<O, B, Q, BC> {

	protected static final String PROP_IDS = "i";
	protected static final String PROP_SMAP = "s";
	protected static final String PROP_VAL = "d";
	
	private static transient final Logger logger = Logger
    .getLogger(AbstractKnnGraph.class);

	/**
	 * Number of connections per node.
	 */
	protected int localk = 15;
	
	protected float t = 1.4f;

	protected transient NeoService neo;
	protected int seeds = 14;
	
	public void setT(float t){
		this.t = t;
	}

	protected enum RelTypes implements RelationshipType

	{
		NN
	}

	public AbstractKnnGraph(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount,
			int localk) throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
		this.localk = localk;
	}
	
	/**
	 * Set the seeds to be used in search as value.
	 * @param value
	 */
	public void setSeeds(int value){
		this.seeds = value;
	}
	

	/**
	 * Buckets do not have duplicates to preserve space.
	 */
	protected void initByteArrayBuckets() throws OBException {
		OBStorageConfig conf = new OBStorageConfig();
		conf.setTemp(false);
		conf.setDuplicates(false);
		conf.setBulkMode(!isFrozen());
		this.Buckets = fact.createOBStore("Buckets", conf);
	}

	/**
	 * Fill data into node n from bucket. Node n will be modified.
	 * 
	 * @param n
	 *            Node where to fill the data in.
	 * @param bucket
	 *            The bucket where we will take the data.
	 * @return
	 * @throws OBException 
	 */
	protected void fillNode(Node n, B bucket) throws OBException {
		// add the bucket id to the given node.
		// all the objects here have the same smap vector.
		long[] idsToStore = null;
		if (n.hasProperty(PROP_IDS)) {
			long[] ids = (long[]) n.getProperty(PROP_IDS);
			idsToStore = new long[ids.length + 1];
			System.arraycopy(ids, 0, idsToStore, 0, ids.length);
		} else {
			idsToStore = new long[1];
		}
		idsToStore[idsToStore.length - 1] = bucket.getId();
		n.setProperty(PROP_IDS, idsToStore);

		// fill the remaining type specific stuff
		fillNodeAux(n, bucket);
	}

	protected abstract void fillNodeAux(Node n, B Bucket) throws OBException;

	
	/**
	 * Return a node id from a gray code
	 * @param grayCode
	 * @return -1 if the code was not found.
	 * @throws IllegalArgumentException 
	 * @throws OBException 
	 */
	protected long getNodeId(byte[] grayCode) throws IllegalArgumentException, OBException{
		byte[] b = Buckets.getValue(grayCode);
		if(b == null){
			return -1;
		}
		return ByteConversion.bytesToLong(b);
	}
	
	/**
	 * TODO: test if the element exists in the DB. Put this method as insertBucketBulk
	 */
	protected OperationStatus insertBucket(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		OperationStatus res = exists(object);
		if(res.getStatus() == Status.NOT_EXISTS){
			return insertBucketBulk(b, object);
		}else{
			return res;
		}
	}
	
	
	
	@Override
	public void freeze() throws AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OBStorageException, OutOfRangeException, OBException, PivotsUnavailableException, IOException {

		super.freeze();
		CloseIterator<TupleLong> it = A.processAll();
		int i = 0;
		while(it.hasNext()){
			TupleLong t = it.next();
			O o = super.bytesToObject(t.getValue());
			B b = getBucket(o);
			b.setId(t.getKey());
			if(i % 100 == 0){
				logger.info("Bulk insert: " + i);
			}
			insertBucketBulk(b,o);
			i ++;
		}
		it.closeCursor();
		
		logger.debug("Gray size: " + Buckets.size());
	}
	
	
	

	@Override
	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		super.init(fact);
		
		neo = new EmbeddedNeo((new File(fact.getFactoryLocation(), "neo")).getAbsolutePath() );
	}
	
	

	
	
	
	
	
	
	@Override
	public void close() throws OBException {
		super.close();
		neo.shutdown();
	}

	

	

}
