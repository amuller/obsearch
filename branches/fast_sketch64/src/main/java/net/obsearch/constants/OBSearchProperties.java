package net.obsearch.constants;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.obsearch.exception.OBException;

import org.apache.log4j.PropertyConfigurator;
/**
 * See the file obsearch.properties for comments
 * on each value. 
 *
 */
public class OBSearchProperties {

	/**
	 * Properties loaded from the properties file.
	 */
	private static Properties properties = null;

	/**
	 * Return the properties from the test properties file.
	 * 
	 * @return the properties from the test properties file
	 * @throws IOException
	 *             If the file cannot be read.
	 */
	public static Properties getProperties() throws IOException {
		if (properties == null) { // load the properties only once
			InputStream is = OBSearchProperties.class
					.getResourceAsStream(File.separator + "obsearch.properties");
			properties = new Properties();
			properties.load(is);
		}

		return properties;
	}

	public static int getACacheSize() throws OBException {
		return getIntProperty("cache.a.size");	
	}
	
	public static int getLHandlesCacheSize() throws OBException {
		return getIntProperty("l.cache.handles.size");	
	}
	
	public static int getBucketsCacheSize() throws OBException {
		return getIntProperty("cache.Buckets.size");	
	}
	
	public static int getBCacheSize() throws OBException {
		return getIntProperty("cache.B.size");	
	}
	
	public static long getBDBCacheSize() throws OBException{
		return getLongProperty("bdb.cache.size");	
	}
	
	
	
	public static String getStringProperty(String prop) throws OBException{
		try{
			return getProperties().getProperty(prop);
		}catch(Exception e){
			throw new OBException(e);
		}
    	
    }
	

	public static int getIntProperty(String prop) throws OBException{
		try{
			return Integer.parseInt(getProperties().getProperty(prop));
		}catch(Exception e){
			throw new OBException(e);
		}
    	
    }
	
	public static long getLongProperty(String prop) throws OBException{
		try{
			return Long.parseLong(getProperties().getProperty(prop));
		}catch(Exception e){
			throw new OBException(e);
		}
    	
    }
}
