package net.obsearch.index.utils;

import java.util.HashSet;
import java.util.Set;

import org.opt4j.benchmark.DoubleCopyDecoder;
import org.opt4j.benchmark.DoubleString;
import org.opt4j.core.problem.Creator;
import org.opt4j.core.problem.Decoder;
import org.opt4j.core.problem.Evaluator;
import org.opt4j.core.problem.ProblemModule;

import com.google.inject.Inject;
/**
 * Optimizing module for finding optimal config parameters.
 * 
 *
 */
public class OBOptimizerModule extends ProblemModule {
	private static Creator<DoubleString> creator;
	private static Evaluator<DoubleString> eval;
	private static Class<? extends Decoder<DoubleString, DoubleString>> decoder;
	
	public OBOptimizerModule(){
		
	}
	/**
	 * Creates an OB optimizer module with a double creator and an instance
	 * for evaluator (it doesn't make sense to create anything since the evaluator will
	 * be called on top of the Index).
	 * @param creator double creator
	 * @param eval evaluator
	 */
	
	public OBOptimizerModule(Creator<DoubleString> creator, Evaluator<DoubleString> eval){
		this.creator = creator;
		this.eval = eval;
		decoder = DoubleCopyDecoder.class;
	}

	@Override
	protected void configure() {
		
		Set<Class<?>> classes = new HashSet<Class<?>>() {
			{				
				add(decoder);
			}
		};

		for (Class<?> clazz : classes) {
			bind(clazz).in(SINGLETON);
		}

		bind(Creator.class).toInstance(creator);
		bind(Decoder.class).to(decoder);
		bind(Evaluator.class).toInstance(eval);

	}

}
