package net.obsearch.utils.bytes;

import java.nio.ByteBuffer;

import net.obsearch.constants.ByteConstants;

/**
 * Adds additional utility classes used to manipulate byte arrays.
 * @author amuller
 *
 */
public class ByteBufferFactoryConversion extends ByteConversion {

    /**
     * Estimates the number of bytes necessary to contain the given
     * number of <primitive type> to store
     * @param bytes # of bytes to store.
     * @param shorts # of shorts to store.
     * @param ints # of ints to store.
     * @param longs # of longs to store.
     * @param floats # of floats to store.
     * @param doubles # of doubles to store.
     */
    public static int estimateByteBuffer(int bytes, int shorts, int ints, int longs, int floats, int doubles){
        int size = 0;
        size += ByteConstants.Byte.getSize() * bytes;
        size += ByteConstants.Short.getSize() * shorts;
        size += ByteConstants.Int.getSize() * ints;
        size += ByteConstants.Long.getSize() * longs;
        size += ByteConstants.Float.getSize() * floats;
        size += ByteConstants.Double.getSize() * doubles;
        return size;
    }
    
    public static ByteBuffer createByteBuffer(int bytes, int shorts, int ints, int longs, int floats, int doubles){
        int size = estimateByteBuffer(bytes, shorts, ints, longs, floats, doubles);
        return createByteBuffer(size);
    }
    
}
