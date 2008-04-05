package org.ajmm.obsearch.example;

public class BlockGeneration {
    
    public static void main(String[] args){
        long i = 0;
        long start = System.currentTimeMillis();
        while(i < Integer.MAX_VALUE){
            i++;
        }
        
        System.out.println("Time:" + (System.currentTimeMillis() - start));
    }

}
