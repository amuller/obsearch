package net.obsearch.index.bucket;

import java.util.Arrays;

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
 * ObjectInBucketShort extends {@link ObjectInBucket} by adding an SMAP vector used to minimize
 * the number of distance computations required per object.
 * @author Arnoldo Jose Muller Molina
 */

public class ObjectInBucketShort
        extends ObjectInBucket implements Comparable<ObjectInBucketShort>{

    /**
     * SMAP vector of the object.
     */
    private short[] smapVector;

    /**
     * Creates a new bucket short with
     * @param bucket
     *                Bucket number.
     * @param level
     *                Level within the hash table.
     * @param smapVector
     *                The distances of the corresponding object and the pivots
     *                for the given level.
     * @param exclusionBucket
     *                If true, the corresponding object is in the exclusion
     *                zone.
     * @param id Optional id of the given object. Not always used.
     */
    public ObjectInBucketShort(long bucket, short[] smapVector,
             long id) {
        super(bucket,id);
        this.smapVector = smapVector;
    }
    
    /**
     * Constructor used when you only want to return the smap and the id 
     * of an object (for searching purposes), use with care!
     * @param smapVector
     * @param id
     */
    public ObjectInBucketShort(short[] smapVector,
            long id) {
        this(-1L,smapVector, id);
    }

    /**
     * @return the smapVector
     */
    public short[] getSmapVector() {
        return smapVector;
    }
    
    /**
     * Returns true if the given smap vector ( {@link ObjectInBucketShort}) is
     * equal to this.
     * @return true if both vectors are equal.
     */
    public boolean smapEqual(ObjectInBucketShort other){
        /*assert this.getBucket() == other.getBucket();
        assert this.getStorageBucket() == other.getStorageBucket();
        assert this.isExclusionBucket() == other.isExclusionBucket();*/
        return Arrays.equals(this.smapVector, other.smapVector);
    }

    /**
     * @param smapVector
     *                the smapVector to set
     */
    public void setSmapVector(short[] smapVector) {
        this.smapVector = smapVector;
    }
    
    public int getPivotSize(){
        return smapVector.length;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(ObjectInBucketShort o) {
        if(smapVector[0] < o.smapVector[0]){
            return -1;
        }else if (smapVector[0] > o.smapVector[0]){
            return 1;
        }else{
            return 0;
        }
    }
    
    public String toString(){
        return Arrays.toString(smapVector);
    }

}
