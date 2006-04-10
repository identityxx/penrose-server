package org.safehaus.penrose;

import org.safehaus.penrose.config.PenroseConfig;
import org.apache.log4j.Logger;

import java.net.URL;
import java.util.Properties;

/**
 * @author Endi S. Dewata
 */
public class PenroseFactory {

    public static Logger log = Logger.getLogger(PenroseFactory.class);

    public static PenroseFactory instance;

    public static PenroseFactory getInstance() throws Exception {
        if (instance == null) {

            String factoryClassName = PenroseFactory.class.getName();
            try {
                ClassLoader cl = PenroseFactory.class.getClassLoader();
                URL url = cl.getResource("org/safehaus/penrose/penrose.properties");
                Properties properties = new Properties();
                properties.load(url.openStream());

                String s = properties.getProperty("org.safehaus.penrose.factory");
                if (s != null) factoryClassName = s;

            } catch (Exception e) {
                // ignore
            }

            //log.debug("Initializing "+factoryClassName);
            Class clazz = Class.forName(factoryClassName);
            instance = (PenroseFactory)clazz.newInstance();
        }
        return instance;
    }

    public Penrose createPenrose(PenroseConfig penroseConfig) throws Exception {
        return new Penrose(penroseConfig);
    }

    public Penrose createPenrose(String home) throws Exception {
        return new Penrose(home);
    }

    public Penrose createPenrose() throws Exception {
        return new Penrose();
    }
}
