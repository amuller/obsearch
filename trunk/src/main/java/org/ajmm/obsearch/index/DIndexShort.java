package org.ajmm.obsearch.index;

import java.util.ArrayList;
import java.util.Iterator;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.index.d.BucketContainer;
import org.ajmm.obsearch.index.d.ObjectBucket;
import org.ajmm.obsearch.index.d.ObjectBucketShort;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class DIndexShort < O extends OBShort >
        extends
        AbstractDIndex < O, ObjectBucketShort, OBQueryShort < O >, BucketContainerShort > {

    @Override
    protected ObjectBucketShort getBucket(O object, int level) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected ObjectBucketShort getBucket(O object) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void initSpecializedStorageDevices() throws OBStorageException {
        // TODO Auto-generated method stub

    }

    

}
