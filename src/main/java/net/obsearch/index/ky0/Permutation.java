package net.obsearch.index.ky0;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.index.perm.impl.PerDouble;

public final class Permutation {
	
	private List<Inversion> inversions;
	private int[] permutations;
	private int[] inversionTableRight;
	private int[] inversionTableLeft;
	private int height;
	/**
	 * Creates a permutatoin from a list of pivot distances.
	 * @param pivotDist
	 * @throws OBException 
	 */
	public Permutation(double[] pivotDist) throws OBException {
		
		
		
		
	}
	
	public int getHeight(){
		return height;
	}
	
	public Permutation(int[] perms){
		permutations = perms;
		populateRest();
	}
	
	/**
	 * 6 4 6 7 8 2
	 * 5 6 7 8 9 1
	 * Inversion table
	 */
	
	private void populateRest(){
		inversionTableRight = new int[permutations.length];
		inversionTableLeft = new int[permutations.length];
		int rRight = 0;
		int rLeft = 0;
		// populate inversion table
		int i = 0;
		for(int d : permutations){
			int k = countBiggerToRight(d, permutations, i);
			inversionTableRight[i] = k;
			rRight += k;
			i++;
		}
		
		i = 0;
		for(int d : permutations){
			int k = countBiggerToLeft(d, permutations, i);
			inversionTableLeft[i] = k;
			rLeft += k;
			i++;
		}
		
		// now we generate the inversions
		/*inversions = new ArrayList<Inversion>();
		int cx = 0;
		for(int id1 : permutations){
			int j = cx + 1;
			while(j < permutations.length){
				int id2 = permutations[j];
				if(id1 > id2){
					inversions.add(new Inversion(id1, id2));
				}
				j++;
			}
			cx++;
		}
		assert inversions.size() == rRight;
		// sort the inversions
		Collections.sort(inversions);
		*/
		assert rRight == rLeft;

		height = rRight;
	}
	
	public int height(Permutation p){
		
		return Math.abs(this.height - p.height);
	}
	
	public int spearman(Permutation p){
		int i = 0;
		int res  = 0;
		for(int per : permutations){
			int index = p.findIndex(per);
			res += Math.abs(index -  i);
			i++;
		}
		return res;
	}
	
	public String toString(){
		return Arrays.toString(this.permutations);
	}
	
	public int left(Permutation p){
		return l1(this.inversionTableLeft, p.inversionTableLeft);
	}
	public int right(Permutation p){
		return l1(this.inversionTableRight, p.inversionTableRight);
	}
	
	public int lr(Permutation p){
		return left(p) + right(p);
	}
	
	private int l1(int[] a, int[] b){
		int i = 0;
		int res = 0;
		while(i < a.length){
			res += Math.abs(a[i] - b[i]);
			i++;
		}
		return res;
	}
	
	/**
	 * find index of the given pivot
	 * @param i
	 * @return
	 */
	private int findIndex(int pivot){
		int i  = 0;
		for(int p : permutations){
			if(p == pivot){
				return i;
			}
			i++;
		}
		assert false : "bad logic";
		return -1;
	}
	
	private int countBiggerToRight(int perm, int[] perms, int index){
		int i = index;
		int res = 0;
		while(i < perms.length){
			if(perms[i] < perm){
				res++;
			}
			i++;
		}
		return res;
	}
	
	private int countBiggerToLeft(int perm, int[] perms, int index){
		int i = index;
		int res = 0;
		while(i >= 0){
			if(perms[i] > perm){
				res++;
			}
			i--;
		}
		return res;
	}
	
	public List<Inversion> getInversions() {
		return inversions;
	}
	
	
}
