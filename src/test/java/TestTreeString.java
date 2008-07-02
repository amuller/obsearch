import java.util.Arrays;

import org.ajmm.obsearch.example.ted.DMRW;
import org.ajmm.obsearch.example.ted.SliceAST;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.example.TreeToString;
import org.ajmm.obsearch.example.ted.SliceFactory;

import cern.colt.matrix.ObjectMatrix1D;
import cern.colt.matrix.impl.SparseObjectMatrix1D;

import junit.framework.TestCase;


public class TestTreeString
        extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    public void testBasic() throws Exception{
        doIt("a(b(c),d(e))", "a(b(c, d(e)))");
        doIt("a(b(c(k(g(h(m))))))", "a(b(c(d(g(h(m))))))");
        doIt("a(b(c(k(g(h(m))))))", "a(b(c(d(g(h(m))))))");
        doIt("p(db(bb(), eb()), qa(gb(),db(bb())))", "p(db(bb()), qa(gb(),db(bb())))");
        
                doIt("sum(phi(first, second),2)",
        "sumb(phi(first,second, gomi, gomi, gomi), 2) ");
        
        doIt("a (d, e)",
        "c (f, d (x,z,k))");
        
        doIt("a ( b (c ,d, e), f (g, d, e, h))",
        "m ( b (c ,d), z (g, d, e))");
        
        
        doIt("a()",
       "a(b)");
        
        doIt("a()",
        "m ( b (c ,d), z (g, d, e))");
        
        
        doIt("m ( z (g, d, e),b (c ,d) )",
        "m ( b (c ,d), z (g, d, e))");
        
        doIt("m ( z (g, d, e,k),b (c ,d,d) )",
        "m ( b (c ,d), z (g, d, e))");
      
        
        doIt("m ( z (g, d, e,k),b (c ,d,d), c (f, d (x,z,k)) )",
        "m (sumb(phi(first,second, gomi, gomi, gomi), 2),  z (g, d, e,k),b (c ,d,d) )");

        doIt("z(b(j,k),c(m,n))", "a(c(k,j),b(n,m))");
        
        doIt("a(b(m(j(z,m),k),n),c)", "a(b,d(k,l(k,m(l,s(a,p)))))");
        
     //doIt("fvirtualInvokeExpr(fmethodRef(), fcast(ftype(),finterfaceInvoke(fmethodRef(), fcast(ftype(),finterfaceInvoke(fmethodRef(), finterfaceInvoke(fmethodRef(), fcast(ftype(),finterfaceInvoke(fmethodRef(), fvirtualInvokeExpr(fmethodRef()), fcast(ftype(),finterfaceInvoke(fmethodRef(), finterfaceInvoke(fmethodRef(), finterfaceInvoke(fmethodRef(), fvirtualInvokeExpr(fmethodRef())))))))))), fintConstant(0))))",
     //        "fvirtualInvokeExpr(fmethodRef(), fcast(ftype(),finterfaceInvoke(fmethodRef(), fcast(ftype(),finterfaceInvoke(fmethodRef(), finterfaceInvoke(fmethodRef(), fcast(ftype(),finterfaceInvoke(fcast(ftype(),finterfaceInvoke(fmethodRef(), finterfaceInvoke(fmethodRef(), finterfaceInvoke(fmethodRef(), fvirtualInvokeExpr(fmethodRef())))))))))), fintConstant())))");
       
     // it is changing the tree.
     doIt("a(b,c(b,e))", "a(c(b,b),e)");
     doIt("a(b,c(b,e))", "a(e,c(b,b))");
     doIt("a(b(f,c(d,e)))", "a(b(f,c(g,e)))");
    }
    
    
    private void doIt(String a, String b) throws Exception{
        SliceAST tree1 = SliceFactory.createSliceASTLean(a);
        System.out.println(TreeToString.depth(tree1));        
        SliceAST tree2 = SliceFactory.createSliceASTLean(b);
        System.out.println(TreeToString.depth(tree2));
        long time = System.currentTimeMillis();
        SparseObjectMatrix1D treeArray1 = TreeToString.treeToString(tree1);
        SparseObjectMatrix1D  treeArray2 = TreeToString.treeToString(tree2);
        //System.out.println(Arrays.deepToString(treeArray1));
      // System.out.println(Arrays.deepToString(treeArray2));
        //TreeToString.printSparseVector(treeArray1);
        //TreeToString.printSparseVector(treeArray2);
        
        ObjectMatrix1D [] result = TreeToString.clean(treeArray1, treeArray2);
        ObjectMatrix1D clean1 = result[0];
        ObjectMatrix1D clean2 = result[1];
        time = System.currentTimeMillis();
        System.out.println("Created in: "  + (System.currentTimeMillis() - time));
        //String clean2[] = TreeToString.cleanString(treeArray2, treeArray1);
        TreeToString.printSparseVector(clean1);
        TreeToString.printSparseVector(clean2);
         time = System.currentTimeMillis();
         
        
        System.out.println("LEV: " + TreeToString.LD(clean1, clean2));
        System.out.println("Time: " + (System.currentTimeMillis() - time));
        DMRW t = new DMRW();
        time = System.currentTimeMillis();
        System.out.println("TED: " +  t.ted(SliceFactory.createSliceForest(a), SliceFactory.createSliceForest(b)));
        System.out.println("Time: " + (System.currentTimeMillis() - time));
        OBSlice amtd = new OBSlice(a);
        OBSlice bmtd = new OBSlice(b);
        time = System.currentTimeMillis();
        System.out.println("MTD: " +  amtd.distance(bmtd));
        System.out.println("Time: " + (System.currentTimeMillis() - time));
        
    }

    
}
