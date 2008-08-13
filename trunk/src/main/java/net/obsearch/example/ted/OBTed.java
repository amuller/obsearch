package net.obsearch.example.ted;



import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import net.obsearch.example.SliceParseException;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBShort;




public class OBTed  implements OBShort {


    /**
     * The root node of the tree.
     */
    protected SliceForest tree;
    private int hashCode;

    /**
     * Default constructor must be provided by every object that implements the
     * interface OB.
     */
    public OBTed() {

    }

    /**
     * Creates an slice object.
     * @param slice
     *                A string representation of a tree.
     */
    public OBTed(final String slice) throws OBException {
        this.updateTree(slice);
        assert tree.equalsTree( this.parseTree(tree.toFuriaChanTree())) : "This: " + tree.toFuriaChanTree() + " slice: " + slice + " size: " + tree.getSize();
    }

    /**
     * Calculates the distance between two trees. TODO: traverse the smallest
     * tree.
     * @param object
     *                The other object to compare
     * @see net.obsearch.OB#distance(net.obsearch.OB,
     *      net.obsearch.result.Dim)
     * @throws OBException
     *                 if something wrong happens.
     * @return A short that indicates how similar or different the trees are.
     */
    public final short distance(final OBShort object) throws OBException {
        OBTed b = (OBTed) object;
        return distance(b.tree, this.tree);        
    }

    /**
     * Calculates the distance between trees a and b.
     * @param a
     *                The first tree (should be smaller than b)
     * @param b
     *                The second tree
     * @return The distance of the trees.
     */
    private final short distance(SliceForest a, SliceForest b) {
        
            DMRW t = new DMRW();
            return (short)t.ted(a, b);
    }

    /**
     * Internal method that updates the Tree from the String
     * @throws OBException
     */
    protected final void updateTree(String x) throws OBException {
        tree = parseTree(x);
        this.hashCode = tree.toFuriaChanTree().hashCode();
    }

    private final SliceForest parseTree(String x) throws SliceParseException {
        try {
            
            
            return SliceFactory.createSliceForest(x);
        } catch (Exception e) {
            throw new SliceParseException(x, e);
        }
    }
    
    

    /**
     * Returns the size (in nodes) of the tree.
     * @return The size of the tree.
     * @throws OBException
     *                 If something goes wrong.
     */
    public final int size() throws OBException {
        return tree.getSize();
    }

    /**
     * @return A String representation of the tree.
     */
    public final String toString() {
        String res = ":)";
        try {
            res = tree.toFuriaChanTree() + "|" + tree.getSize() + "|";
        } catch (Exception e) {
            assert false;
        }
        return res;
    }

    /**
     * Re-creates this object from the given byte stream
     * @param in
     *                A byte stream with the data that must be loaded.
     * @see net.obsearch.Storable#load(com.sleepycat.bind.tuple.TupleInput)
     */
    public final void load(byte[] in) throws OBException, IOException {
        
        updateTree(new String(in));
    }

    /**
     * Stores this object into the given byte stream.
     * @param out
     *                The byte stream to be used
     * @see net.obsearch.Storable#store(com.sleepycat.bind.tuple.TupleOutput)
     */
    public final byte[] store() throws IOException{
        return tree.toFuriaChanTree().getBytes();
    }

    /**
     * Returns true of this.tree.equals(obj.tree). For this distance function
     * this.distance(obj) == 0 implies that this.equals(obj) == true
     * @param obj
     *                Object to compare.
     * @return true if this == obj
     */
    public final boolean equals(final Object obj) {
        return this.tree.equalsTree(((OBTed) obj).tree);
    }

    /**
     * A hashCode based on the string representation of the tree.
     * @return a hash code of the string representation of this tree.
     */
    public final int hashCode() {
        return this.hashCode;
    }

}
