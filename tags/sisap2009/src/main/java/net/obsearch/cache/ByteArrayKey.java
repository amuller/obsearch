package net.obsearch.cache;

import java.util.Arrays;

public class ByteArrayKey {
	 private byte[] key;
     private int hashCode;
     
     public ByteArrayKey(byte[] key){
         this.key = key;
         this.hashCode = Arrays.hashCode(key);
     }

     /* (non-Javadoc)
      * @see java.lang.Object#equals(java.lang.Object)
      */
     @Override
     public boolean equals(Object obj) {
         return Arrays.equals(key, ((ByteArrayKey)obj).key);
     }

     /* (non-Javadoc)
      * @see java.lang.Object#hashCode()
      */
     @Override
     public int hashCode() {
         return hashCode;
     }
     
     public byte[] getKey(){
    	 return key;
     }
}
