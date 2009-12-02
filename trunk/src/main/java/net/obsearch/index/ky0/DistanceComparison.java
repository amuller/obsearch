package net.obsearch.index.ky0;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Compares two distance functions
 * @author amuller
 *
 */
public class DistanceComparison {
	private String x;
	private String y;
	private SortedMap<Double, StaticBin1D> map = new TreeMap<Double, StaticBin1D>();
	
	public DistanceComparison(String x, String y){
		this.x = x;
		this.y = y;
	}
	
	public void add(double x, double y){
		StaticBin1D s = map.get(x);
		if(s == null){
			s = new StaticBin1D();
			map.put(x, s);
		}
		s.add(y);
	}
	
	public void save(File file) throws IOException{
		FileWriter f = new FileWriter(file);
		f.write("# X: " + x + " Y: " + y + "\n");		
		f.write("# X, avg, std dev, min, max, count \n");		
		for(Map.Entry<Double, StaticBin1D> s : map.entrySet()){
			StaticBin1D y = s.getValue();
			f.write(s.getKey() + "\t" + y.mean() + "\t" + y.standardDeviation() + "\t" + y.min() + "\t" + y.max() + "\t" + y.size() +"\n");			
		}		
		f.close();
	}
	
}
