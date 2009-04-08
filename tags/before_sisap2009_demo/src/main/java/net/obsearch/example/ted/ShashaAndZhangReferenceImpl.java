package net.obsearch.example.ted;

import java.util.HashMap;

import net.obsearch.index.utils.IntegerHolder;



public class ShashaAndZhangReferenceImpl extends AbstractTED{
	
	
	
	
    private HashMap<String, IntegerHolder> cache;
	
	public ShashaAndZhangReferenceImpl(){
		init();
	}
	
	protected void init(){
		cache = new HashMap<String, IntegerHolder>();
		// we don't want to be filling in the default case lots of times...
		put(SliceFactory.createEmptySliceForest(), SliceFactory.createEmptySliceForest(), 0);
	}
	
	protected void init(int n, int m){		
		cache = new HashMap<String, IntegerHolder>(n * m);
		// we don't want to be filling in the default case lots of times...
		put(SliceFactory.createEmptySliceForest(), SliceFactory.createEmptySliceForest(), 0);
	}
	
	/** 
	 * puts the given value into the cache
	 * @param a
	 * @param b
	 * @param value
	 */
	protected final  void put(final SliceForest a, final SliceForest b, int value){		
		put(makeKey(a,b), value);
	}
	protected final void put(String k, int value){
		cache.put(k, new IntegerHolder(value));
	}
	
	/**
	 * returns an integer with the given value of the cache, otherwise returns -1
	 * @param k
	 * @return
	 */
	protected final int  get(String k){
		IntegerHolder r =  cache.get(k);
		if(r == null){
			return -1;
		}else{
			return r.getValue();
		}
	}
	
	protected final String makeKey(final SliceForest a, final SliceForest b){
		StringBuilder str = new StringBuilder(a.hashString());
		str.append(",");
		str.append(b.hashString());
		return str.toString();
	}
	
	public int ted(final SliceForest a, final SliceForest b){
		init(a.getSize(), b.getSize());
		return tedAux(a,b);
	}
	
	public int tedAux(final SliceForest a, final SliceForest b){
		int res;
		String key = makeKey(a, b);
		int v = get(key); // get catched value. I was catched. no need to do anything.
		if(v != -1){
			res = v;
		}else if(a.isNull() && b.isNull()){
			res = 0;
		}else if(!a.isNull() && b.isNull()){
			res = tedAux(a.deleteRightTreeNode(), b ) + DeleteCost;
		}else if(a.isNull() && ! b.isNull()){
			res = tedAux(a, b.deleteRightTreeNode()) + DeleteCost;
		}else{
			int v1 = tedAux(a.deleteRightTreeNode(), b) + DeleteCost;
			int v2 = tedAux(a, b.deleteRightTreeNode()) + DeleteCost ;
			int v3 = tedAux(a.deleteRootOnRightTreeAndGetRightTree(), b.deleteRootOnRightTreeAndGetRightTree()) + 
			tedAux(a.deleteRightTree() , b.deleteRightTree()) + renameCost(a.getRightTree(), b.getRightTree());	
			res = min(v1,v2,v3);
		}
		// I am not catched, store the catched value.
		if(v == -1){
			put(key, res);
		}
		return res;
	}
	
	public int tedSliceAST(SliceAST a, SliceAST b) throws Exception{
		throw new Exception("cannot call this method in this class");
	}
}
