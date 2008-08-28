import static org.junit.Assert.*;

import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.exception.OBException;
import org.junit.Before;
import org.junit.Test;


public class TestOBSlice {

    String[][] trees = { {"a(b,c(e(f),d))", "a(b(c(e(f),d)))"},
            {"a(b,c(e(f),d))", "a(b,c(e(f),e))"}, 
            {"a(b(e(f)),c(e(f),d))", "a(b(e(f),c(e(f),d)))"},
            {"a(b(c(e,d)), f)", "a(b,c(e,d(f)))"},
            {"a(b(c(e(f),d)))", "a(b(c(e(f),e)))"}, 
            
            {"$(V(U(M(),X()),p(Q(J(S(T(),R(S(T(),U(M(),X())),P(8.0))),b(8)),b(4)), Q(a(),b(-1)))),%(p(S(T(),%(K(Y(), W(), p(W(), Q(a(),b(6)), Q(a(),b(2)))),b(255))), S(T(),x(a(), b(1)))),b(1)))",
            "Q(U(S(T(),d(Y(), S(T(),Z(Y(), Z(Y(), W()))))),X()),Q(U(S(T(),d(Y(), S(T(),Z(Y(), Z(Y(), W()))))),X()),U(S(T(),d(Y(), S(T(),Z(Y(), Z(Y(), W()))))),X())))"
        },
        {"p(O(), U(M(),X()), d(Y(), z(T(), d(Y()))))",  "d(Y(), J(W(),d(Y())))"},
        {
            "d(Y(), K(Y(), d(Y()), K(Y(), d(Y()), p(K(Y(), d(Y()), b(0)), K(Y(), d(Y()), p(K(Y(), d(Y()), a()), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(p(a(), a()),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(p(a(), a()),b(1))))), K(Y(), d(Y()), p(p(K(Y(), d(Y()), a()), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(p(a(), a()),b(1)))), p(K(Y(), d(Y()), a()), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(p(a(), a()),b(1)))))), K(Y(), d(Y()), p(p(K(Y(), d(Y()), a()), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(p(a(), a()),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(a(),b(1)))), p(K(Y(), d(Y()), a()), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(K(Y(), d(Y()), Q(a(),b(1))),b(1))), K(Y(), d(Y()), Q(p(a(), a()),b(1))), K(Y(), d(Y()), Q(a(),b(1))), K(Y(), d(Y()), Q(a(),b(1)))))), Q(a(),b(2)), Q(a(),b(2))))))",
            "d(Y(), p(K(Y(), S(T(),Z(Y(), d(Y(), p(W(), S(T(),Z(Y(), a(), c()))), b(0), U(M(),X())), c()))), S(T(),Z(Y(), d(Y(), p(W(), S(T(),Z(Y(), a(), c()))), b(0), U(M(),X())), c())), O(), d(Y(), d(Y(), b(1), L(d(Y()),b(1)))), S(T(),Z(Y(), a(), c())), S(T(),Z(Y(), p(d(Y(), S(T(),Z(Y(), d(Y(), p(W(), S(T(),Z(Y(), a(), c()))), b(0), U(M(),X())), c())), b(0), U(M(),X())), S(T(),Z(Y(), d(Y(), p(W(), S(T(),Z(Y(), a(), c()))), b(0), U(M(),X())), c()))), c())), d(Y(), a()), S(T(),Z(Y(), d(Y(), a(), b(0), O()), c())), d(Y(), a()), S(T(),Z(Y(), d(Y(), a(), b(0), O()), c()))), b(0), U(M(),X()))"

        }
            
            
    };
    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testDistance()  throws OBException{
        
        for (String[] j:trees){
            OBSlice x1 = new OBSlice(j[0]);
            OBSlice x2 = new OBSlice(j[1]);
            long time = System.currentTimeMillis();            
            short r = x1.distance(x2);
            long timeRes = System.currentTimeMillis() - time;
            System.out.println("r=" + r + " time: " + timeRes + " sizes: " + x1.size() + " , " + x2.size());
        }
        
        
    }

}
