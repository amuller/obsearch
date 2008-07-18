package net.obsearch.example.ted;

import java.util.List;

public interface SliceForest {

	public abstract SliceForest deleteRightTreeNode();
	
	public abstract SliceForest deleteLeftTreeNode();
	
	
	public abstract boolean isLeftHeavy();
	
	public abstract SliceForestStd deleteRootOnLeftTreeAndGetLeftTree();
	
	public abstract SliceForestStd deleteLeftTree();
	
	public abstract boolean isTree();
	
	public abstract SliceForest deleteRightTree();

	public abstract SliceASTForStandardTed getRightTree();

	public abstract SliceASTForStandardTed getLeftTree();
	
	public abstract void calculateHeavyPath();
	
	public abstract List<SliceForest> topLight();
	
	public abstract int getSize();
	
	public abstract boolean isNull();
	
	public abstract void updateIdInfo();
	public abstract void updateContains();
	public abstract void updateDescendant();
	
	public abstract String prettyPrint();

	/**
	 * return the rightmost tree of the Forest without the root node
	 * 
	 */
	public abstract SliceForest deleteRootOnRightTreeAndGetRightTree();
	
	public abstract String hashString();
	
	public String toFuriaChanTree();
	
	public boolean equalsTree(SliceForest o);

}