package net.obsearch;

import java.util.logging.Logger;

import org.junit.Test;
public class TestQs {
	static Logger logger = Logger.getLogger(TestQs.class.getSimpleName());
	
	
	public static void combiCounter(char[] chars){
		long counter = 1;
		long end = (long)Math.pow(2, chars.length);
		while(counter <= end){
			printChars(counter, chars);
			counter++;
		}
	}
	
	public static void printChars(long counter, char[]chars){
		int i = 0;
		while(i < chars.length){
			if((counter & (1 << i)) != 0){
				System.out.print(chars[i]);
			}
			i++;
		}
		System.out.println();
	}
	
	public static void combiRecursive(char[] chars){
		 combiRecursiveAux(chars, 0, new StringBuilder(), new boolean[chars.length]);
	}
	
	
	public static void combiRecursiveAux(char[] chars, int start, StringBuilder buf, boolean[] flags){
		int i = start;
		while(i < chars.length){
			if(flags[i]){
				i++;
				continue;
			}
			buf.append(chars[i]);
			System.out.println(buf.toString());
			flags[i] = true;			
			combiRecursiveAux(chars, start, buf, flags);
			flags[i] = false;
			buf.setLength(buf.length() - 1);				
			i++;
		}
	}
	
	@Test
	public void testCombi(){
		//combiCounter(new char []{'w', 'x', 'y', 'z'});
		combiRecursive(new char []{'w', 'x', 'y', 'z'});
	}
	
}
