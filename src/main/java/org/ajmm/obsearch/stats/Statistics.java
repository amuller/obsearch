package org.ajmm.obsearch.stats;

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
    }
    
    
}
