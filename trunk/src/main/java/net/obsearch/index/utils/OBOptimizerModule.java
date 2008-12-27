package net.obsearch.index.utils;

import java.util.HashSet;
import java.util.Set;

import org.opt4j.benchmark.DoubleCopyDecoder;
import org.opt4j.benchmark.DoubleString;
import org.opt4j.core.problem.Creator;
import org.opt4j.core.problem.Decoder;
import org.opt4j.core.problem.Evaluator;
import org.opt4j.core.problem.ProblemModule;
/**
 * Optimizing module for finding optimal config parameters.
 * 
 *
 */
public class OBOptimizerModule extends ProblemModule {
	private Class<? extends Creator<DoubleString>> creator;
	private Evaluator<DoubleString> eval;
	private Class<? extends Decoder<DoubleString, DoubleString>> decoder;
	/**
	 * Creates an OB optimizer module with a double creator and an instance
	 * for evaluator (it doesn't make sense to create anything since the evaluator will
	 * be called on top of the Index).
	 * @param creator double creator
	 * @param eval evaluator
	 */
	public OBOptimizerModule(Class<? extends Creator<DoubleString>> creator, Evaluator<DoubleString> eval){
		this.creator = creator;
	}

	@Override
	protected void configure() {
		
		Set<Class<?>> classes = new HashSet<Class<?>>() {
			{
				add(creator);
				add(decoder);
			}
		};

		for (Class<?> clazz : classes) {
			bind(clazz).in(SINGLETON);
		}

		bind(Creator.class).to(creator);
		bind(Decoder.class).to(decoder);
		bind(Evaluator.class).toInstance(eval);

	}

}
