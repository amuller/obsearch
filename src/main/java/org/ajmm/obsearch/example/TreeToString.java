package org.ajmm.obsearch.example;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.example.ted.SliceAST;

import cern.colt.list.IntArrayList;
import cern.colt.list.ObjectArrayList;
import cern.colt.matrix.ObjectMatrix1D;
import cern.colt.matrix.impl.SparseObjectMatrix1D;
public class TreeToString {
    
    public static SparseObjectMatrix1D  treeToString(SliceAST a){
        //SparseObjectMatrix1D result = new SparseObjectMatrix1D((int)Math.pow(2, depth(a)));      
        SparseObjectMatrix1D result = new SparseObjectMatrix1D((int)Math.pow(2, depth(a)), a.getSize(), 0.25, 0.50);  
        treeToStringAux(result, a, 0);
        return result;
    }
    
    public static void treeToStringAux(SparseObjectMatrix1D j, SliceAST node, int i){
        if(node != null){
            j.set(i ,node.getText());
            treeToStringAux(j, (SliceAST)node.getFirstChild(), (2 * i) + 1);
            treeToStringAux(j, (SliceAST)node.getNextSibling(), (2 * i) + 2);
        }
    }
    
    
    public static short LD (ObjectMatrix1D s, ObjectMatrix1D t) {
        int d[][];
        int n; 
        int m;
        int i; 
        int j; 
        String s_i;
        String t_j;
        int cost; 


          n = s.size();
          m = t.size();
          if (n == 0) {
            return (short)m;
          }
          if (m == 0) {
            return (short)n;
          }
          d = new int[n+1][m+1];


          for (i = 0; i <= n; i++) {
            d[i][0] = i;
          }

          for (j = 0; j <= m; j++) {
            d[0][j] = j;
          }


          for (i = 1; i <= n; i++) {

            s_i = (String)s.getQuick(i - 1);

            for (j = 1; j <= m; j++) {

              t_j = (String)t.getQuick(j - 1);

              if(s_i == null && t_j == null){
                  cost = 0;
              }else if(s_i == null || t_j == null){
                  cost = 1;
              }
              else if (s_i.equals( t_j)) {
                cost = 0;
              }
              else {
                cost = 1;
              }


              d[i][j] = min (d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1] + cost);

            }

          }
          return (short)d[n][m];

        }
    
    // clean string a based on string b.
    public static  String[] cleanString(String[] a, String[]b){
        List<String> toClean = new LinkedList<String>();
        int i = 0;
        while(i < a.length){
            if(a[i] != null ||  (i < b.length && b[i] != null)){
                toClean.add(a[i]);
            }
            i++;
        }
        String[] result = new String[toClean.size()];
        i = 0;
        for(String x : toClean){
            result[i] = x;
            i++;
        }
        return result;
    }
    
    public static  ObjectMatrix1D[] clean(SparseObjectMatrix1D a, SparseObjectMatrix1D b){
        IntArrayList indexListA = new IntArrayList();
        ObjectArrayList valueListA = new ObjectArrayList();
        IntArrayList indexListB = new IntArrayList();
        ObjectArrayList valueListB = new ObjectArrayList();
        a.getNonZeros(indexListA, valueListA);
        b.getNonZeros(indexListB, valueListB);
        
        SparseObjectMatrix1D resultA = new SparseObjectMatrix1D(indexListA.get(indexListA.size()-1) +1);
        SparseObjectMatrix1D resultB = new SparseObjectMatrix1D(indexListB.get(indexListB.size()-1) +1);
        
        int sourceA = 0;
        int sourceB = 0;
        int destA = 0;
        int destB = 0;
        int lastStoredA = 0;
        int lastStoredB = 0;
        while(sourceA < indexListA.size() || sourceB < indexListB.size()){
            boolean ok = sourceA  < indexListA.size() && sourceB < indexListB.size();
            boolean addA = ok && indexListA.get(sourceA) <= indexListB.get(sourceB);
            boolean addB = ok && indexListB.get(sourceB) <= indexListA.get(sourceA);
            
            if(addA || (!ok && sourceA  < indexListA.size())){
                resultA.set(destA, valueListA.get(sourceA));
                lastStoredA = destA;
                destA++;
                sourceA++;
            }else{
                 destA = destA + 1;
            }
            if(addB || (! ok &&sourceB < indexListB.size()) ){
                resultB.set(destB, valueListB.get(sourceB));
                lastStoredB = destB;
                destB++;
                sourceB++;
            }  else{
                 destB = destB + 1;
            }        
        }     
        ObjectMatrix1D[] result = new ObjectMatrix1D[2];
        result[0] = resultA.viewPart(0, lastStoredA+1);
        result[1] = resultB.viewPart(0, lastStoredB+1);
        
        return result;
    }
    
    public static void printSparseVector(ObjectMatrix1D a){
        IntArrayList indexListA = new IntArrayList();
        ObjectArrayList valueListA = new ObjectArrayList();
        a.getNonZeros(indexListA, valueListA);
        int i = 0;
        StringBuffer res = new StringBuffer();
        while(i < indexListA.size()){
            res.append(valueListA.get(i));
            res.append("-");
            res.append(indexListA.get(i));
            res.append(",");
            i++;
        }
        System.out.println(res.toString() +  " size: " + a.size());
    }
    
    
    public static int depth(SliceAST a){
        return depthAux(a, 0) ;
    }
    
    public static int depthAux(SliceAST a, int i){
        if(a == null){
            return i;
        }else{
            return Math.max(depthAux((SliceAST)a.getFirstChild(), i +1), depthAux((SliceAST) a.getNextSibling(), i + 1));
        }
    }
    
    private static int min (int a, int b, int c) {
        int mi;

          mi = a;
          if (b < mi) {
            mi = b;
          }
          if (c < mi) {
            mi = c;
          }
          return mi;

        }


}
