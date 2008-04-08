package org.ajmm.obsearch.example.ted;






abstract public class AbstractTED implements TED {

	protected  int min(int a, int b, int c){
		return Math.min(Math.min(a, b), c);
	}
	
	protected  int min(int a, int b, int c, int d){
		return min(min(a,b,c),d);
	}
	
	protected  int min(int a, int b){
		return Math.min(a, b);
	}


	protected int renameCost(SliceAST a, SliceAST b){
		if(a.getText().equals(b.getText())){
			return 0;
		}
		else{
			return RenameCost;
		}
	}

}
