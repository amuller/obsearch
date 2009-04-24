package net.obsearch.storage.cuckoo;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import net.obsearch.exception.OBException;


public class ByteArrayFixedTest extends ByteArrayStorageTest{
	
	private int RECORD_SIZE = 100;
	private  Random r = new Random();
	
	@Override
	public  byte[] generateByteArray(){
		byte[] rec = new byte[RECORD_SIZE];
		r.nextBytes(rec);
		return rec;
	}
	@Override
	public ByteArray createStorage(File file) throws FileNotFoundException, OBException{
		System.out.println("Creating fixed record");
		return new ByteArrayFixed(RECORD_SIZE, file);
	}

}
