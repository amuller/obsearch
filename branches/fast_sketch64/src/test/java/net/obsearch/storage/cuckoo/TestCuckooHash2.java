package net.obsearch.storage.cuckoo;

import java.io.File;
import java.io.IOException;

import net.obsearch.exception.OBException;


public class TestCuckooHash2 extends TestCuckooHash{
	
	protected HardDiskHash createHash(File location) throws IOException, OBException{
		return new CuckooHash2(TEST_PERF_SIZE , location, new Murmur64(), new Jenkins64() );
	}

}
