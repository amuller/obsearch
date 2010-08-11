package net.obsearch.utils;

/**
 * A pair of objects
 * @author amuller
 *
 * @param <A>
 * @param <B>
 */
public class Pair<A,B> {
	private A a;
	private B b;
	
	
	
	public Pair(A a, B b) {
		super();
		this.a = a;
		this.b = b;
	}
	public A getA() {
		return a;
	}
	public void setA(A a) {
		this.a = a;
	}
	public B getB() {
		return b;
	}
	public void setB(B b) {
		this.b = b;
	}
	
	public boolean equals(Object o){
		Pair<A,B> po =(Pair<A,B>)o;
		return po.getA().equals(a) && po.getB().equals(b);
	}
}
