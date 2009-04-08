package net.obsearch.index.utils.medians;

import cern.colt.Arrays;

public class MedianCalculatorShort {
	
	
		private int[] values;
		private int total =  0;
		public MedianCalculatorShort(short max){
			values = new int[max];
		}
		
		public void add(short val){
			values[val]++;
			total++;
		}
		
		public short median(){
			int half = total /2;
			short i = 0;
			int cx = 0;
			while(i < values.length){
				if(values[i] == 0){
					i++;
					continue;
				}
				cx += values[i];
				if(cx >= half){
					break;
				}
				i++;
			}
			return i;
		}
		
		public String toString(){
			return Arrays.toString(values);
		}
	

}
