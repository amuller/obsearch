package net.obsearch.index.ky0;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class DistanceHistogram {
	private SortedMap<Double, Integer> data;
	private String description;
	public DistanceHistogram(String x){
		data = new TreeMap<Double, Integer>();
		description = x;
	}
	
	public void add(double distance){
		Integer i = data.get(distance);
		if(i == null){
			i = 0;
		}
		data.put(distance, i + 1);
		
	}
	
	
	public void save(File file) throws IOException{
		FileWriter f = new FileWriter(file);
		f.write("# X: " + description + "\n");				
		for(Map.Entry<Double, Integer> s : data.entrySet()){
			int y = s.getValue();
			f.write(s.getKey() + "\t" + s.getValue() + "\n");			
		}		
		f.close();
	}
	
}
