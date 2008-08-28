package org.ajmm.obsearch.example.ted;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.utils.OBFactory;

public class OBTedFactory implements OBFactory<OBTed> {
    public static int maxSliceSize = 30;
    @Override
    public OBTed create(String x) throws OBException {
       return new OBTed(x); 
    }

    @Override
    public boolean shouldProcess(OBTed obj) throws OBException {
         return obj.size() <= maxSliceSize;
    }

}
