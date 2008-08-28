package org.ajmm.obsearch.cache;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.example.OBString;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.utils.OBFactory;

public class OBStringFactory implements OBFactory<OBString> {

    @Override
    public OBString create(String x) throws OBException {
        return new OBString(x);
    }

    @Override
    public boolean shouldProcess(OBString obj) throws OBException {
        return true;
    }

}
