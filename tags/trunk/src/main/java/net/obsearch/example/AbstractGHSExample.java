package net.obsearch.example;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.kohsuke.args4j.Option;

import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;

/**
 * Adds some parameters required by GHS indexes
 * @author amuller
 *
 */
public abstract class AbstractGHSExample extends AbstractExampleGeneral {
	
	
	@Option(name = "-ep", usage = "Target error", aliases = { "--error_position" })
	protected float CompoundError = 0.001f;
	
	
	
	@Option(name = "-kA", usage = "Alpha used during search time", aliases = { "--k_alpha" })
	protected float alpha = 1.0f;
	
	
	

	public AbstractGHSExample(String[] args) throws IOException,
			OBStorageException, OBException, IllegalAccessException,
			InstantiationException, PivotsUnavailableException {
		super(args);
	}

	

}
