package org.ajmm.obsearch.example.ted;

import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;



import antlr.BaseAST;
import antlr.CommonAST;
import antlr.Token;
import antlr.collections.AST;
/**
 * This class provides extra functionality required by tree edit distance algorithms and the like
 * @author amuller
 *
 */
public class SliceASTForStandardTed extends SliceASTIds {

	
	private int ttype = Token.INVALID_TYPE;
    
    
    private SliceASTForStandardTed heavy = null;
    
    public int updateDecendantInformation(){
    	decendants = 0;
    	SliceASTForStandardTed n = (SliceASTForStandardTed)this.getLeftmostChild();
    	while(n != null){
    		decendants += n.updateDecendantInformation();
    		n = (SliceASTForStandardTed)n.getNextSibling();
    	}
    	return decendants + 1;
    }
    
    /*public int getDescendants(){
    	int decendants = 0;
    	SliceAST n = this.getLeftmostChild();
    	while(n != null){
    		decendants += n.getDescendants();
    		decendants++;
    		n = (SliceAST)n.getNextSibling();
    	}
    	return decendants;
    }*/
    
    public int getDescendants(){
    	if(decendants == -1){
    		this.updateDecendantInformation();
    	}
    	return decendants;
    }
    
    public int getSize(){
    	return getDescendants() + 1;
    }
    /**
     * toplight never 
     * @return
     */
    public LinkedList<SliceASTForStandardTed> topLight(){
    	if(heavy == null){
    		updateHeavyPathInformation();
    	}
    	LinkedList<SliceASTForStandardTed> res = new LinkedList<SliceASTForStandardTed>();
    	topLightAux(res);
    	return res;
    }
    
    protected void topLightAux(LinkedList<SliceASTForStandardTed> m){
    		SliceASTForStandardTed n = this.getLeftmostChild();
    		while(n != null){
    			if(n != this.heavy){
        		    m.add(n);
    			}else{
    				n.topLightAux(m);
    			}
    			n = (SliceASTForStandardTed)n.getNextSibling();
        	}
    }
    
    public SliceASTForStandardTed getHeavy(){
    	return this.heavy;
    }
    
    public void updateHeavyPathInformation(){
    	
    	if(getLeftmostChild() != null){
    	SliceASTForStandardTed biggest = this.getLeftmostChild();
    	biggest.updateHeavyPathInformation();
    	SliceASTForStandardTed n = (SliceASTForStandardTed) this.getLeftmostChild().getNextSibling();
    	while(n != null){
    		if(biggest.getDescendants() < n.getDescendants()){
    			biggest = n;
    		}
    		n.updateHeavyPathInformation();
    		n = (SliceASTForStandardTed)n.getNextSibling();
    	}
    	this.heavy = biggest;
    	}
    }
    
  
    
    public SliceASTForStandardTed findFirstNodeThatMatches(String label){
    	SliceASTForStandardTed result = null;
    	if(this.text.equals(label)){
    		result = this;
    	}else{
    		SliceASTForStandardTed n = this.getLeftmostChild();
        	while(n != null && result == null){
        		result = n.findFirstNodeThatMatches(label);
        		n = (SliceASTForStandardTed)n.getNextSibling();
        	}
    	}
    	return result;
    }
    
    /** Get the token text for this node */
    public String getText() {
        return text;
    }

    /** Get the token type for this node */
    public int getType() {
        return ttype;
    }

    public void initialize(int t, String txt) {
        setType(t);
        setText(txt);
    }

    public void initialize(AST t) {
        setText(t.getText());
        setType(t.getType());
    }

    public SliceASTForStandardTed() {
    }
    public SliceASTForStandardTed(int t, String txt) {
        initialize(t, txt);
    }
    public SliceASTForStandardTed(Token tok) {
        initialize(tok);
    }
    public SliceASTForStandardTed(SliceASTForStandardTed t) {
        initialize(t.ttype, t.text);
    }
    

    public void initialize(Token tok) {
        setText(tok.getText());
        setType(tok.getType());
    }

    /** Set the token text for this node */
    public void setText(String text_) {
        text = text_;
    }

    /** Set the token type for this node */
    public void setType(int ttype_) {
        ttype = ttype_;
    }
	

	
	
	public SliceASTForStandardTed getLeftmostChild(){
		return (SliceASTForStandardTed)super.getFirstChild();
	}

		
	/** Print out a child-sibling tree in LISP notation */
    public String prettyPrint() {
        SliceASTForStandardTed t = this;
        String ts = "";
        if (t.getFirstChild() != null) ts += " (";
        ts += " " + this.toString();
        if (t.getFirstChild() != null) {
            ts += ((SliceASTForStandardTed)t.getFirstChild()).prettyPrint();
        }
        if (t.getFirstChild() != null) ts += " )";
        if (t.getNextSibling() != null) {
            ts += ((SliceASTForStandardTed)t.getNextSibling()).prettyPrint();
        }
        return ts;
    }		
    

    /*
     * little speed up to the normal equalsTree method
     * @see antlr.BaseAST#equalsTree(antlr.collections.AST)
     */
    public boolean equalsTree(AST t) {
    	SliceASTForStandardTed j = (SliceASTForStandardTed)t;
    	if(j.getSize() != this.getSize()){ // little speed up! ;)
    		return false;
    	}else{
    		return super.equalsTree(t);
    	}
    }
    
    /*
     * 1 = equal
     * 0 = not equal
     * 2 = equal if we rename the root
     * 3 = children not equal but the same root
     * @see antlr.BaseAST#equalsTree(antlr.collections.AST)
     */
    public int detailedTreeComparison(AST t) {
    	SliceASTForStandardTed j = (SliceASTForStandardTed)t;
    	if(j.getSize() != this.getSize()){ // little speed up! ;)
    		if(this.equals(t)){
    			return 3;
    		}else{
    			return 0;
    		}
    	}else{
    		return detailedTreeAux(t);
    	}
    }
    
    /** Is tree rooted at 'this' equal to 't'?  The siblings
     *  of 'this' are ignored.
     *  
     */
    protected int detailedTreeAux(AST t) {
    	int res = -1;
        // check roots first.
        
        // if roots match, do full list match test on children.
        if (this.getFirstChild() != null) {
            if (this.getFirstChild().equalsList(t.getFirstChild())){ res = 2;}else{res = 0;}
        }
        // sibling has no kids, make sure t doesn't either
        else if (t.getFirstChild() != null) {
            res = 0;
        }
        
        if (this.equals(t) && ( res == 2 || res == -1)) {
        	res = 1;
        }
        if(res == -1){
        	res = 0;
        }
        if(this.equals(t) && res == 0){
        	res = 3; 
        }
        
        return res;
    }
    
   
    // TODO refactor all these things. hack to keep the unit tests working
    // dummy
    /**public int updateDecendantInformation(){
    	assert false;
    	return -1;
    }*/
    // dummy
    /*public void updateIdInfo(){
    	assert false;
    }
    // dummy
    public void updateContains(){
    	assert false;
    }
    // dummy
    public int getId(){
    	assert false;
    	return -1;
    }
    // dummy
    public boolean containsNode(int i){
    	assert false;
    	return false;
    }*/
    
    
}
