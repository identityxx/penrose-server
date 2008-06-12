package org.safehaus.penrose.service;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.commons.digester.Digester;

import java.net.URL;
import java.io.IOException;
import java.io.File;
import java.io.FilenameFilter;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public ServiceReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/service/service.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/service/service-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public ServiceConfig read(File serviceDir) throws Exception {

        ServiceConfig serviceConfig = new ServiceConfig(serviceDir.getName());

        File serviceInf = new File(serviceDir, "SERVICE-INF");

        File serviceXml = new File(serviceInf, "service.xml");
        digester.push(serviceConfig);
		digester.parse(serviceXml);
        digester.pop();

        //log.debug("Classpath:");

        File classesDir = new File(serviceInf, "classes");
        if (classesDir.exists()) {
            URL url = classesDir.toURL();
            //log.debug(" - "+url);
            serviceConfig.addClassPath(url);
        }

        File libDir = new File(serviceInf, "lib");
        if (libDir.isDirectory()) {
            File files[] = libDir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for (File file : files) {
                URL url = file.toURL();
                //log.debug(" - "+url);
                serviceConfig.addClassPath(url);
            }
        }

        return serviceConfig;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}

