import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class Experiments {
	
	public static int DB_SIZE = 10000000;
	
	public static void main(String[] args) throws IOException{
		StaticBin1D stats = new StaticBin1D ();
		StaticBin1D readStats = new StaticBin1D ();
		long i  = 0;
		while( i < DB_SIZE){
			long time = System.currentTimeMillis();
			doIt(i);
			stats.add(System.currentTimeMillis() - time);
			i++;
		}
		System.out.println("Write Stats...");
		System.out.println(stats);
		System.out.println("Reading...");
		i = 0;
		while( i < DB_SIZE){
			long time = System.currentTimeMillis();
			doIt(i);
			readStats.add(System.currentTimeMillis() - time);
			i++;
		}
		System.out.println(readStats);
	}
	
	public static void doIt(long id) throws IOException{
		File f = new File(pathToId(id));
		f.getParentFile().mkdirs();
		RandomAccessFile acc = new RandomAccessFile(f, "rw");
		acc.writeLong(id);
		acc.close();
	}
	
	public static long read(long id) throws IOException{
		RandomAccessFile acc = new RandomAccessFile(pathToId(id), "rw");
		long res = acc.readLong();
		acc.close();
		return res;
	}
	
	public static String pathToId(long id){
		StringBuilder b = new StringBuilder();
		b.append("source");
		b.append(File.separator);
		String l = Long.toString(id);
		int i = 0;
		int cx = 1;
		while(i < l.length()){
			if(cx == 3){
				b.append(File.separator);
				cx = 1;
			}
			b.append(l.charAt(i));
			cx++;
			i++;
		}
		b.append(".r");
		return b.toString();
	}

}
