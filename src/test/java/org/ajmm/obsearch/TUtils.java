/**
 *
 */
package org.ajmm.obsearch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;


/**
 * Class: TestOB Test utils class
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class TUtils {

    private static Properties testProperties = null;

    public static Properties getTestProperties() throws IOException {
        if (testProperties == null) { // load the properties only once
            InputStream is = TUtils.class
                    .getResourceAsStream("/test.properties");
            testProperties = new Properties();
            testProperties.load(is);
            // configure log4j only once too
            PropertyConfigurator.configure(testProperties
                    .getProperty("test.log4j.file"));
        }

        return testProperties;
    }

}
