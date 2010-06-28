package net.obsearch.example.lev;

import java.io.IOException;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBShort;

public  class OBString implements OBShort {
	
	protected String str;
	
	public OBString(){
		
	}
	
	public OBString(String x) throws OBException{
		//OBAsserts.chkAssert(x.length() < Short.MAX_VALUE, "Cannot exceed: " + Short.MAX_VALUE);
		this.str = x;
	}
	
	public int length(){
		return str.length();
	}

	@Override
	public short distance(OBShort object) throws OBException {
		OBString o = (OBString) object;
		int d[][];
	    int n; 
	    int m;
	    int i; 
	    int j; 
	    char s_i;
	    char t_j;
	    int cost; 
	    String s = str;
	    String t = o.str;

	      n = s.length();
	      m = t.length();
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

	        s_i = s.charAt(i - 1);

	        for (j = 1; j <= m; j++) {

	          t_j = t.charAt(j - 1);


	          if (s_i == t_j) {
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
	
	private int min (int a, int b, int c) {
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
	
	@Override
	public boolean equals(Object o){
		return str.equals(((OBString)o).str);
	}
	
	

	@Override
	public int hashCode() {
		return str.hashCode();
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		str = new String(input);

	}

	@Override
	public byte[] store() throws OBException, IOException {
		return str.getBytes();
	}

	public String toString(){
		return str;
	}
}
