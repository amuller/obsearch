package net.obsearch.index.knngraph;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.TUtils;

import org.junit.Test;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Transaction;
import org.neo4j.api.core.Traverser;

import cern.colt.Arrays;


public class TestNeo {
	
	public enum Types implements RelationshipType {
        KNOWS
    }

	
	@Test
	public void testBasic() throws IOException{
		File dir = new File( TUtils.getTestProperties().getProperty(
		        "test.neo.location"));
		Directory.deleteDirectory(dir);
        assertTrue(!dir.exists());
        assertTrue(dir.mkdirs());
		NeoService neo = new EmbeddedNeo( dir.getAbsolutePath()) ;
		
		Transaction tx = neo.beginTx();

		
		try
		{
			
			Node n = neo.createNode();
			n.setProperty("name", "n");
			n.setProperty("smap", new byte[]{1,2,3});
			Node n2 = neo.createNode();
			n2.setProperty("name", "n2");
			n2.setProperty("smap", new byte[]{4,5,6});

			n.createRelationshipTo(n2, Types.KNOWS);
			n2.createRelationshipTo(n, Types.KNOWS);
			
			
			
		    tx.success();
		    
		    Traverser nodes = n.traverse(
		    		   Traverser.Order.BREADTH_FIRST,
		    		   StopEvaluator.END_OF_GRAPH,
		    		   ReturnableEvaluator.ALL,
		    		   Types.KNOWS,
		    		   Direction.OUTGOING );
		    
		    for(Node k : nodes){
		    	if(k.hasProperty("name")){
		    		System.out.println(k.getProperty("name"));
		    		System.out.println(Arrays.toString((byte[])k.getProperty("smap")));
		    	}
		    }

		}
		finally
		{
		   tx.finish();
		   neo.shutdown();
		}


	}

}
