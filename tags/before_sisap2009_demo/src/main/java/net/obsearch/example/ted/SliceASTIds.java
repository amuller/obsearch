package net.obsearch.example.ted;

import java.util.BitSet;

import net.obsearch.index.utils.IntegerHolder;

import antlr.collections.AST;



/*
 * a bigger slice node  that contains extra information
 */
public class SliceASTIds extends SliceAST {
	
	private int id = -1; // id used to uniquely identify this node
    private BitSet contains;
    
    
    public BitSet containedNodes(){
    	assert contains != null;
    	return contains;
    }
    
    public void updateContains(){
    	if (contains == null){
    		this.updateIdInfo();    	
    		updateContainsAux(null);
    	}
    }
    
    public boolean containsNode(int i){
    	assert contains != null;
    	return this.contains.get(i);
    }
    
    protected void updateContainsAux(BitSet parent){
    	SliceASTIds n = (SliceASTIds)this.getLeftmostChild();
    	BitSet me = new BitSet();
		while(n != null){
			me.set(n.getId());
			n.updateContainsAux(me);
			n = (SliceASTIds)n.getNextSibling();
    	}
		this.contains = me;
		if(parent != null){
			parent.or(me);// update the parent
		}
    }
    
    
    
    public int getId(){
    	assert id != -1;
    	return id;
    }
    
    public void updateIdInfo(){
    	updateIdInfoAux(new IntegerHolder(0));
    }
    
    public void updateIdInfoAux(IntegerHolder i){
    	this.id = i.getValue();
    	SliceASTIds n = (SliceASTIds)this.getLeftmostChild();
		while(n != null){
			i.inc();
			n.updateIdInfoAux(i);
			n = (SliceASTIds)n.getNextSibling();
    	}
    }
    
    /*
     * little speed up to the normal equalsTree method
     * this is wrong!!! TODO review
     * @see antlr.BaseAST#equalsTree(antlr.collections.AST)
     */
   /** public boolean equalsTree(AST t) {
    	SliceASTIds j = (SliceASTIds)t;
    	if(j.getSize() != this.getSize() || ! j.contains.equals(this.contains)){ // little speed up! ;)
    		return false;
    	}else{
    		return super.equalsTree(t);
    	}
    }**/
    
    
    
}
