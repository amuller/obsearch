package net.obsearch.stats;

import hep.aida.bin.StaticBin1D;

import java.util.HashMap;
import java.util.Map.Entry;

import net.obsearch.index.utils.IntegerHolder;
import net.obsearch.index.utils.StatsUtil;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Statistics gathered during the execution of a program.
 * @author Arnoldo Jose Muller Molina
 */

public class Statistics {
	
	/**
	 * Additional counters.
	 */
	private HashMap<String, IntegerHolder> extra = new HashMap<String, IntegerHolder>();
	
	
	/**
	 * Additional information regarding the index.
	 */
	private HashMap<String, StaticBin1D> extraStats = new HashMap<String, StaticBin1D>();
	
	
	/**
	 * Additional information regarding the index.
	 */
	private HashMap<String, Object> extraObjects = new HashMap();

    /**
     * # of distance computations.
     */
    private long distanceCount = 0;
    /**
     * Number of SMAP vectors searched.
     */
    private long smapCount = 0;
    
    /**
     * Amount of reads.
     */
    private long diskAccessCount = 0;
    
    /**
     * Queries performed so far.
     */
    private long queryCount = 0; 
    
    /**
     * Amount of data read.
     */
    private long dataRead = 0;
    
    /**
     * # of buckets read
     */
    private long bucketsRead = 0;
    
    
    
    /**
     * Adds x to the current value of {@link #bucketsRead}.
     * @param x
     */
    public void incBucketsRead(long x){
        bucketsRead += x;
    }
    
    /**
     * Increment the # of buckets read.
     */
    public void incBucketsRead(){
        bucketsRead++;
    }
    
    /**
     * Increment an extra value.
     * @param key
     */
    public void incExtra(String key){
    	incExtra(key, 1);
    }
    
    /**
     * Increment an extra value.
     * @param key
     */
    public void incExtra(String key, int value){
    	IntegerHolder i = extra.get(key);
    	if(i == null){
    		i = new IntegerHolder(0);
    		extra.put(key, i);
    	}
    	i.add(value);
    }
    
    /**
     * Increment an extra value.
     * @param key
     */
    public void setExtra(String key, int value){
    	IntegerHolder i = extra.get(key);
    	if(i == null){
    		i = new IntegerHolder(0);
    		extra.put(key, i);
    	}
    	i.setValue(value);
    }
    
    /**
     * Increment an extra value.
     * @param key
     */
    public int getExtra(String key){
    	IntegerHolder i = extra.get(key);
    	if(i == null){
    		return 0;
    	}
    	return i.getValue();
    }
    
    /**
     * Increment distance count by distance.
     */
    public void incDistanceCount(long distance) {
        distanceCount+= distance;
    }
    
    /**
     * Increment distance count.
     */
    public void incDistanceCount() {
        distanceCount++;
    }
    
    /**
     * Increment smap count.
     */
    public void incSmapCount() {
        smapCount++;
    }
    
    /**
     * Increment smap count by count.
     * @param count the amount that will be increased
     */
    public void incSmapCount(long count) {
        smapCount += count;
    }
    
    /**
     * Increment disk access count.
     */
    public void incDiskAccessCount() {
        diskAccessCount++;
    }
    
    /**
     * Increment query count.
     */
    public void incQueryCount() {
        queryCount++;
    }
    
    /**
     * Increment data read
     * @param dataRead The amount read.
     */
    public void incDataRead(long dataRead) {
        this.dataRead += dataRead;
    }
    

    /**
     * @return the distanceCount
     */
    public long getDistanceCount() {
        return distanceCount;
    }

    /**
     * @param distanceCount the distanceCount to set
     */
    public void setDistanceCount(long distanceCount) {
        this.distanceCount = distanceCount;
    }

    /**
     * @return the smapCount
     */
    public long getSmapCount() {
        return smapCount;
    }

    /**
     * @param smapCount the smapCount to set
     */
    public void setSmapCount(long smapCount) {
        this.smapCount = smapCount;
    }

    /**
     * @return the diskAccessCount
     */
    public long getDiskAccessCount() {
        return diskAccessCount;
    }

    /**
     * @param diskAccessCount the diskAccessCount to set
     */
    public void setDiskAccessCount(long diskAccessCount) {
        this.diskAccessCount = diskAccessCount;
    }

    /**
     * @return the queryCount
     */
    public long getQueryCount() {
        return queryCount;
    }

    /**
     * @param queryCount the queryCount to set
     */
    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }

    /**
     * @return the dataRead
     */
    public long getDataRead() {
        return dataRead;
    }

    /**
     * @param dataRead the dataRead to set
     */
    public void setDataRead(long dataRead) {
        this.dataRead = dataRead;
    } 
    
    /**
     * Reset all the counters to 0.
     */
    public void resetStats(){
        distanceCount = 0;
        smapCount = 0;        
        diskAccessCount = 0;        
        queryCount = 0;         
        dataRead = 0;
        this.bucketsRead = 0;
        extra = new HashMap<String, IntegerHolder>();
				extraObjects = new HashMap();
				extraStats = new HashMap<String, StaticBin1D>();
    }
    
    /**
     * Add the given set of statistics.
     * @param name Name of the statistics
     * @param stats The stats.
     */
    public void putStats(String name, StaticBin1D stats){
    	this.extraStats.put(name, stats);
    }
    
    public StaticBin1D getStats(String name){
    	StaticBin1D r = extraStats.get(name);
    	if(r != null){
    		return r;
    	}else{
    		return new StaticBin1D();
    	}
    }
    
    /**
     * Add values to a stats bin, each value
     * added will be considered as a unit and 
     * median, std. dev will be calculated on the set
     * of values added
     * @param name Name of  the stat
     * @param value Value to add
     */
    public void addExtraStats(String name, double value){
    	StaticBin1D s = extraStats.get(name);
    	if(s == null){
    		s = new StaticBin1D();
    		extraStats.put(name, s);
    	}
    	s.add(value);
    }
    
    /**
     * Add arbitrary info.
     * @param name Name of the statistics
     * @param stats The stats.
     */
    public void putObjects(String name, Object stats){
    	this.extraObjects.put(name, stats);
    }
    
    public String toStringSummary(){
    	return "Distances: " + distanceCount +
        " Pivot vector count: " + smapCount +        
        " Disk access count: " + diskAccessCount +        
    " Query count: " + queryCount +         
    " Data read: " + dataRead +
    " Buckets Read: " + this.bucketsRead + " extra: " + extra;
    }
    
    
    
    public long getBucketsRead() {
		return bucketsRead;
	}

	public String toString(){
        StringBuilder res = new StringBuilder( toStringSummary() );
        for(Entry<String, StaticBin1D> e : extraStats.entrySet()){
        	res.append(StatsUtil.prettyPrintStats(e.getKey(), e.getValue()));
        }
        for(Entry<String, Object> e : extraObjects.entrySet()){
        	res.append(e.getKey());
        	res.append("\n");
        	res.append(e.getValue());
        }
        return res.toString();
    }
    
    
}
