package org.ajmm.obsearch.index.utils;

import java.io.File;
import java.io.IOException;

public class Directory {
	
		public static void deleteDirectory(File dbFolder)throws IOException {
			if (!dbFolder.exists()) {
				return;
			}
			File[] files = dbFolder.listFiles();
			for (File f : files) {
				if(f.isDirectory()){
					deleteDirectory(f);
				}else{
					if(! f.delete()){
						throw new IOException("Could not delete: " + f);
					}
				}
			}
			if(! dbFolder.delete()){
				throw new IOException("Could not delete: " + dbFolder);
			}
			if(! dbFolder.exists()){
				throw new IOException("Could not delete: " + dbFolder);
			}			
		}
	
}
