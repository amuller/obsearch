package net.obsearch.example;

public class BlockGeneration {
    
    public static void main(String[] args){
        long i = 0;
        long start = System.currentTimeMillis();
        while(i < Integer.MAX_VALUE){
            i++;
        }
        
        System.out.println("Time:" + (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        char[] res = "Hello mYeah it has charted and they've broken the club/dance charts here! they often chart highly in the dance chart. Ive heard their songs in clubs plenty of times :)".toCharArray();
        System.out.println("Time:" + (System.currentTimeMillis() - start));
        
        start = System.currentTimeMillis();
        String r = new String(res);
        System.out.println("Time:" + (System.currentTimeMillis() - start));
       
    }

}
