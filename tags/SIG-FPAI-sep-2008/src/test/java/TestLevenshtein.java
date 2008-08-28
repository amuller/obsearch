import static org.junit.Assert.*;
import org.ajmm.obsearch.example.OBString;
import org.ajmm.obsearch.exception.OBException;
import org.junit.Before;
import org.junit.Test;


public class TestLevenshtein {

    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testDistance() throws OBException{
        
        testStrings("kitten", "sitten", 1);
        testStrings("sitten", "sittin", 1);
        testStrings("sitting", "sittin", 1);
        testStrings("calar", "cromar", 3);       
        testStrings("Diana King", "Sharon Stone", 9);       
        testStrings("Diana King", "Diana King", 0);       
        testStrings("Step one", "Step three", 4);    
        testStrings("zalar", "calar", 1);   
        testStrings("zalar", "cdlar", 2);   
    }
    
    private void testStrings(String a, String b, int distance) throws OBException{        
        OBString obA = new OBString(a);
        OBString obB = new OBString(b);
        int dist = obA.distance(obB);
        assertEquals(a + " and " + b + " returned: " + dist + " but should have been: " + distance , dist,  distance);
    }

}
