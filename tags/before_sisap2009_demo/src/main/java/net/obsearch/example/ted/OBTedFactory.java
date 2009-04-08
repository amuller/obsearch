package net.obsearch.example.ted;

import net.obsearch.OB;
import net.obsearch.exception.OBException;
import net.obsearch.index.utils.OBFactory;


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
