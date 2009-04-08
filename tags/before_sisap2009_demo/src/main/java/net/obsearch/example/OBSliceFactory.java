package net.obsearch.example;

import net.obsearch.OB;
import net.obsearch.exception.OBException;
import net.obsearch.index.utils.OBFactory;


public class OBSliceFactory implements OBFactory<OBSlice> {

    public static  int maxSliceSize = 300;
    
    
    
    

    /**
     * @return the maxSliceSize
     */
    public int getMaxSliceSize() {
        return maxSliceSize;
    }

    /**
     * @param maxSliceSize the maxSliceSize to set
     */
    public void setMaxSliceSize(int maxSliceSize) {
        this.maxSliceSize = maxSliceSize;
    }

    @Override
    public OBSlice create(String x) throws OBException{
        // TODO Auto-generated method stub
        return new OBSlice(x);
    }

    @Override
    public boolean shouldProcess(OBSlice obj) throws OBException{
        return obj.size() <= maxSliceSize;
    }

}
