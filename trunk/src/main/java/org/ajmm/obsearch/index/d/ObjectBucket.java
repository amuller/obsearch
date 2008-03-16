package org.ajmm.obsearch.index.d;

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
 * ObjectBucket holds a bucket number for an object. Subclasses hold also the SMAP
 * vector with the distances of the object to the pivots of a certain level.
 * @author Arnoldo Jose Muller Molina
 */

public abstract class ObjectBucket {
    
    /**
     * The bucket number of the object.
     */
    private long bucket;
    
    /**
     * If true, this means that we are at the exclusion bucket.
     */
    private boolean exclusionBucket;
    
    /**
     * Level within the hash table; Level is a number > 0 .
     */
    private int level;
    
    /**
     * Id of the object.
     */
    private int id;

    /**
     * @return the bucket
     */
    public long getBucket() {
        return bucket;
    }

    /**
     * @param bucket the bucket to set
     */
    public void setBucket(long bucket) {
        this.bucket = bucket;
    }

    /**
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * @param level the level to set
     */
    public void setLevel(int level) {
        this.level = level;
    }

    /**
     * Creates a new bucket with the given bucket number and
     * the specified level.
     * @param bucket Bucket number.
     * @param exclusionBucket If true, the corresponding object is in the exclusion zone.
     * @param optional id of the object.
     */
    public ObjectBucket(long bucket, int level, boolean exclusionBucket, int id) {
        super();
        this.bucket = bucket;
        this.level = level;
        this.exclusionBucket = exclusionBucket;
        this.id = id;
    }
    
    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

   

    /**
     * @return the exclusionBucket
     */
    public boolean isExclusionBucket() {
        return exclusionBucket;
    }

    /**
     * @param exclusionBucket the exclusionBucket to set
     */
    public void setExclusionBucket(boolean exclusionBucket) {
        this.exclusionBucket = exclusionBucket;
    }
    
    /**
     * Returns the # of pivots.
     * @return
     */
    public abstract int getPivotSize();

}
