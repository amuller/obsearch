package net.obsearch.index.ky0;

public final class Inversion implements Comparable<Inversion> {
	private int a;
	private int b;
	public int getA() {
		return a;
	}
	public void setA(int a) {
		this.a = a;
	}
	public int getB() {
		return b;
	}
	public void setB(int b) {
		this.b = b;
	}
	public Inversion(int a, int b) {
		super();
		this.a = a;
		this.b = b;
	}
	
	public int compareTo(Inversion i ){
		if(a < i.a){
			return -1;
		}else if(a > i.a){
			return 1;
		}else{
			if(b< i.b){
				return -1;
			}else if(b > i.b){
				return 1;
			}else{
				return 0;
			}
		}
	}
	
}
