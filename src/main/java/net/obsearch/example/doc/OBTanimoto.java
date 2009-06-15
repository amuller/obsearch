package net.obsearch.example.doc;

import java.util.Arrays;

import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
/**
 * Implementation of the Tanimoto distance.
 * as described by Zezula et al. in the book:
 * Similarity Search, the metric space approach (Section 3.6, page 14)
 * @author amuller
 *
 */
public class OBTanimoto extends AbstractDocument {

	public OBTanimoto(){
		
	}
	public OBTanimoto(String data) throws OBException {
		super(data);		
	}

	@Override
	public float distance(OBFloat object) throws OBException {
		
		OBTanimoto other = (OBTanimoto)object;
		
		// calculate the dot product
		// ids must be sorted prior doing this.
		long dot = 0;
		int i1 = 0;
		int i2 = 0;
		final int max1 = super.ids.length;
		final int max2 = other.ids.length;
		while(i1 < max1 && i2 < max2){
			if(ids[i1] < other.ids[i2]){
				i1++;
			}else if(ids[i1] > other.ids[i2]){
				i2++;
			}else{ // they are equal				
				dot += counts[i1] * other.counts[i2];
				i1++;
				i2++;
			}			
		}
		
		long normA = euclideanNormSquared();
		long normB = other.euclideanNormSquared();	
		
		double res = 1 - ((dot) / ((normA + normB) - dot));
		assert res >= 0 : " result: " + res + " A: " + super.getName() + " B: " + other.getName();
		assert res <= 1 : " result: " + res;
		
		return (float)res;
	}
	
	public boolean equals(Object o){
		OBTanimoto other = (OBTanimoto)o;
		return Arrays.equals(ids, other.ids) && Arrays.equals(counts, other.counts);
	}
	
	private long euclideanNormSquared(){
		long res = 0;
		for(long d : counts){
			res += d * d;
		}
		return res;
	}

}
