package org.safehaus.penrose.service;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.commons.digester.Digester;

import java.net.URL;
import java.io.Reader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

/**
 * @author Endi Sukma Dewata
 */
public class ServiceReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public ServiceReader() throws Exception {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/service/service.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/service/service-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);

    }

    public ServiceConfig read(String dir) throws Exception {

        String filename = dir+File.separator+"SERVICE-INF"+File.separator+"service.xml";

        ServiceConfig serviceConfig = new ServiceConfig();
        digester.push(serviceConfig);

        Reader reader = new FileReader(filename);
		digester.parse(reader);

        digester.pop();

        return serviceConfig;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}

