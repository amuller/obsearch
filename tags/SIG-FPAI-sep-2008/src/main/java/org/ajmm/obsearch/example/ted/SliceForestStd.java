package org.ajmm.obsearch.example.ted;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import antlr.Token;
import antlr.collections.AST;

/**
 * This class only keeps an artificial root node above all the children to simulate a forest!
 * This is to match the ideas of the papers
 * @author amuller
 *
 */
public class SliceForestStd implements SliceForest  {
	private LinkedList<SliceASTForStandardTed> trees;
	
	private int n = -1;
	private int heavyIndex = -1;
	
	private int getHeavyIndex(){
		if(heavyIndex == -1) { updateCachedData();}
		return heavyIndex;
	}
	
	public boolean isLeftHeavy(){
		
		return getHeavyIndex() == 0;
	}
	
	public int getSize(){
		if(n == -1){ updateCachedData();}
		return n;
	}
	
	public boolean isTree(){
		return trees.size() == 1;
	}
	
	public  void updateDescendant(){
		assert this.isTree();
		this.trees.getLast().updateDecendantInformation();
	}
	
	
	public void updateIdInfo(){
		assert this.isTree();
		this.trees.getLast().updateIdInfo();
	}
	
	public void updateContains(){
		assert this.isTree();
		this.updateIdInfo();
		this.trees.getLast().updateContains();
	}
	
	protected void updateCachedData(){
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		int i =0;
		n = 0;
		int heavy_value = -1;
		while(it.hasNext()){
			SliceASTForStandardTed node = it.next();
			if(node.getDescendants() > heavy_value ){
				heavyIndex = i;
				heavy_value = node.getDescendants();
			}
			i++;
			this.n += node.getSize();
		}
	}
	
	public SliceForestStd(){
		this.treeSet( new LinkedList<SliceASTForStandardTed>());
	}
	
	public SliceForestStd(SliceASTForStandardTed t){
		trees = new LinkedList<SliceASTForStandardTed>();
		this.treeAddRight(t);
	}
	
	private SliceForestStd(LinkedList<SliceASTForStandardTed> t){
		this.treeSet(t);
	}
	
	private void emptyCache(){
		this.heavyIndex = -1;
		this.n = -1;
	}
	
	private void treeSet(LinkedList<SliceASTForStandardTed> t){
		trees = t;
		emptyCache();
	}
	private void treeAddRight(SliceASTForStandardTed t){
		trees.add(t);
		emptyCache();
	}
	
	private void treeAddLeft(SliceASTForStandardTed t){
		trees.addFirst(t);
		emptyCache();
	}
	
	private void treeRemoveRight(){
		trees.removeLast();
		emptyCache();
	}
	
	private void treeRemoveLeft(){
		trees.removeFirst();
		emptyCache();
	}
	
	
	/** Just creates a new array without copying the trees */
	public final SliceForestStd shallowCloneForest(){
		LinkedList<SliceASTForStandardTed> n = new LinkedList<SliceASTForStandardTed>();
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		while(it.hasNext()){
			n.add(it.next());
		}
		return new SliceForestStd(n);
	}
	
	/**
	 * Adds at the end of the forest (right) the given SliceAST and all his sibblings
	 */
	protected final void appendSiblings(SliceASTForStandardTed x){
		SliceASTForStandardTed p = x;
		while(p != null){
			this.treeAddRight(p);
			p = (SliceASTForStandardTed) p.getNextSibling();
		}		
	}
	
	protected final void insertSiblings(SliceASTForStandardTed x){
		SliceASTForStandardTed p = x;
		if(x != null){
			insertSiblings((SliceASTForStandardTed)x.getNextSibling());
			this.treeAddLeft(x);
		}
	}
	
	/* (non-Javadoc)
	 * @see furia.slice.SliceForest#deleteRightTreeNode()
	 */
	public final SliceForestStd deleteRightTreeNode(){
		//assert ! isNull();
		LinkedList<SliceASTForStandardTed> n = new LinkedList<SliceASTForStandardTed>();
		int s = trees.size() - 1;
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		int i = 0 ;
		while(true){
			SliceASTForStandardTed j = it.next();
			if(i < s){
			n.add(j);
			}else{
				break;
			}
			i++;
		}
		SliceForestStd res = new SliceForestStd(n);
		res.appendSiblings( this.getRightTree().getLeftmostChild());
		return res;
	}
	
	public final SliceForestStd deleteLeftTreeNode(){
		//assert ! isNull();
		LinkedList<SliceASTForStandardTed> n = new LinkedList<SliceASTForStandardTed>();
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		int i = 0 ;
		while(it.hasNext()){
			SliceASTForStandardTed j = it.next();
			if(i != 0){
			n.add(j);
			}
			i++;
		}
		SliceForestStd res = new SliceForestStd(n);
		res.insertSiblings( this.getLeftTree().getLeftmostChild());
		return res;
	}
		
	protected final void deleteRightTreeNodeDestructive(){
		//assert ! isNull();
		SliceASTForStandardTed sibling = this.getRightTree().getLeftmostChild();
		this.treeRemoveRight();
		this.appendSiblings( sibling );
	}
	
	protected final void deleteLeftTreeNodeDestructive(){
		//assert ! isNull();
		SliceASTForStandardTed sibling = this.getLeftTree().getLeftmostChild();
		this.treeRemoveLeft();
		this.insertSiblings( sibling );
	}
	
	/* (non-Javadoc)
	 * @see furia.slice.SliceForest#deleteRightTree()
	 */
	public final SliceForestStd deleteRightTree(){
		//assert ! isNull();
		SliceForestStd res = shallowCloneForest();
		res.treeRemoveRight();
		return res;
	}
	
	public final SliceForestStd deleteLeftTree(){
		//assert ! isNull();
		SliceForestStd res = shallowCloneForest();
		res.treeRemoveLeft();
		return res;
	}
	
	/* (non-Javadoc)
	 * @see furia.slice.SliceForest#getRightTree()
	 */
	public final SliceASTForStandardTed getRightTree(){
		return trees.getLast();
	}
	
	/* (non-Javadoc)
	 * @see furia.slice.SliceForest#getLeftTree()
	 */
	public final SliceASTForStandardTed getLeftTree(){
		return trees.getFirst();
	}
	
	public final boolean isNull(){
		return 0 == trees.size();
	}
	
	public void calculateHeavyPath(){
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		while(it.hasNext()){
			SliceASTForStandardTed s = it.next();
			s.updateHeavyPathInformation();
		}
	}
	
	public  List<SliceForest> topLight(){
		LinkedList <SliceForest> res = new LinkedList <SliceForest>();
		int heavy = this.getHeavyIndex();
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		int i =0;
		while(it.hasNext()){
			SliceASTForStandardTed s = it.next();
			if(i != heavy){
				res.add(new SliceForestStd(s));
			}else{
				Iterator<SliceASTForStandardTed> it2 = s.topLight().iterator();
				while( it2.hasNext() ){
				res.add(new SliceForestStd(it2.next()));
				}
			}
			i++;
		}
		return res;
	}
	
	/* (non-Javadoc)
	 * @see furia.slice.SliceForest#deleteRootOnRightTreeAndGetRightTree()
	 */
	public final SliceForestStd deleteRootOnRightTreeAndGetRightTree(){
		SliceForestStd n = new SliceForestStd(getRightTree());
		n.deleteRightTreeNodeDestructive();
		return n;
	}
	
	
	public final SliceForestStd deleteRootOnLeftTreeAndGetLeftTree(){
		SliceForestStd n = new SliceForestStd(getLeftTree());
		n.deleteLeftTreeNodeDestructive();
		return n;
	}
	/* the hash code for this forest is just a list of the hash codes of each of the 
	 * heads of each member of the trees array. That will give a unique representation
	 * of a forest
	 * return a string with the hash code that represents this forest
	 * @see furia.slice.SliceForest#hashString()
	 */
	public final String hashString(){
		StringBuilder res = new StringBuilder();
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		while(it.hasNext()){
			res.append(it.next().hashCode());
			res.append("-");
		}
		return res.toString();
	}
	
	public String prettyPrint(){
		StringBuilder res = new StringBuilder();
		Iterator<SliceASTForStandardTed> it = trees.iterator();
		while(it.hasNext()){
			res.append(it.next().toStringTree());
		}
		return res.toString();
	}
	
	public boolean equalsTree(SliceForest o){
	    SliceForestStd obj = (SliceForestStd) o;
	    boolean res = true;
	    if(trees.size() != obj.trees.size()){
	        return false;
	    }
	    Iterator<SliceASTForStandardTed> it = trees.iterator();
	    Iterator<SliceASTForStandardTed> it2 = obj.trees.iterator();
	    while(it.hasNext()){
	        SliceASTForStandardTed s = it.next();
	        SliceASTForStandardTed s2 = it2.next();
	        if(! s.equalsTree(s2)){
	            res = false;
	            break;
	        }
	    }
	    return res;
	}

	public final String toFuriaChanTree(){
	    if(trees.size() != 1){
                throw new UnsupportedOperationException("This is not a tree, it is a forest :(");
            }
	        StringBuilder sb = new StringBuilder();
	        trees.get(0).toFuriaChanTreeAux(sb);
	        return sb.toString();
	    }

	    
}
