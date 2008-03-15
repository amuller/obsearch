package org.ajmm.obsearch.index.d;

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
 * ObjectBucketShort extends {@link ObjectBucket} by adding an SMAP vector used to minimize
 * the number of distance computations required per object.
 * @author Arnoldo Jose Muller Molina
 */

public class ObjectBucketShort
        extends ObjectBucket {

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
    public ObjectBucketShort(long bucket, int level, short[] smapVector,
            boolean exclusionBucket, int id) {
        super(bucket, level, exclusionBucket,id);
        this.smapVector = smapVector;
    }

    /**
     * @return the smapVector
     */
    public short[] getSmapVector() {
        return smapVector;
    }
    
    /**
     * Returns true if the given smap vector ( {@link ObjectBucketShort}) is
     * equal to this.
     * @return true if both vectors are equal.
     */
    public boolean smapEqual(ObjectBucketShort other){
        assert this.getBucket() == other.getBucket();
        assert this.getStorageBucket() == other.getStorageBucket();
        assert this.isExclusionBucket() == other.isExclusionBucket();
        return Arrays.equals(this.smapVector, other.smapVector);
    }

    /**
     * @param smapVector
     *                the smapVector to set
     */
    public void setSmapVector(short[] smapVector) {
        this.smapVector = smapVector;
    }

}
