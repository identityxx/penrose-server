package org.safehaus.penrose.scheduler;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;

import java.net.URL;
import java.io.File;
import java.io.IOException;

/**
 * @author Endi Sukma Dewata
 */
public class SchedulerReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public SchedulerReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/scheduler/scheduler.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/scheduler/scheduler-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public SchedulerConfig read(File file) throws Exception {
        SchedulerConfig schedulerConfig = new SchedulerConfig();
        read(file, schedulerConfig);
        return schedulerConfig;
    }

    public void read(File file, SchedulerConfig schedulerConfig) throws Exception {

        if (!file.exists()) return;

        digester.push(schedulerConfig);
        digester.parse(file);
        digester.pop();
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
