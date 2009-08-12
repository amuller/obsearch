import hep.aida.bin.StaticBin1D;

import java.util.Random;

import net.obsearch.dimension.DimensionShort;


public class MyObj {
	
	public static Random r = new Random();
	public static void generate(short[] res){
		int i = 0;
		while(i < res.length){
			res[i] = (short)r.nextInt(500);
			i++;
		}
	}
	
	
	
	public final static int test(short[][] data, short[] query){
		//long start = System.currentTimeMillis();
		int res = 0;
		for(short[] j : data){
			res  = res ^ DimensionShort.lInfinite(j, query);
		}
		return res;
		//return System.currentTimeMillis() - start;
	}
	
	public static void main(String args[]){
		final int size = 600000;
		final int width = 32;
		short[][] data = new short[size][width];
		int i = 0;
		while(i < size){
			generate(data[i]); 
			i++;
		}
		short[] query = new short[width];
		generate(query);
		i = 0;
		StaticBin1D s = new StaticBin1D();
		long now = System.currentTimeMillis();
		int res = 0;
		while(i < 1000){			
			res += test(data, query);
			i++;
		}
		long total = System.currentTimeMillis() - now;
		System.out.println("Time in msec: " + total + "res " + res);
		
	}

}
