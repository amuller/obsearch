package org.ajmm.obsearch.example.ted;




public interface TED {
	
	public int DeleteCost = 1;
	
	public int RenameCost = 1;
	
	public  int ted(SliceForest a, SliceForest b) throws Exception;
	
	/**
	 * This ted is used for those implementors who can work directly on the tree
	 * (those who don't need the definition of sliceForest
	 * @param a
	 * @param b
	 * @return
	 * @throws Exception
	 */
	public int tedSliceAST(SliceAST a, SliceAST b) throws Exception;

}
