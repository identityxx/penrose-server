package org.safehaus.penrose.monitor;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.commons.digester.Digester;
import org.safehaus.penrose.service.ServiceConfig;

import java.net.URL;
import java.io.IOException;
import java.io.File;
import java.io.FilenameFilter;

/**
 * @author Endi Sukma Dewata
 */
public class MonitorReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public MonitorReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/monitor/monitor.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/monitor/monitor-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public MonitorConfig read(File monitorDir) throws Exception {

        MonitorConfig monitorConfig = new MonitorConfig(monitorDir.getName());

        File serviceInf = new File(monitorDir, "MONITOR-INF");

        File monitorXml = new File(serviceInf, "monitor.xml");
        digester.push(monitorConfig);
		digester.parse(monitorXml);
        digester.pop();

        //log.debug("Classpath:");

        File classesDir = new File(serviceInf, "classes");
        if (classesDir.exists()) {
            URL url = classesDir.toURL();
            //log.debug(" - "+url);
            monitorConfig.addClassPath(url);
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
                monitorConfig.addClassPath(url);
            }
        }

        return monitorConfig;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}