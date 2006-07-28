package org.safehaus.penrose.config;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;

import java.net.URL;
import java.io.Reader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author Endi S. Dewata
 */
public class PenroseServerConfigReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    URL serverDtdUrl;
    Reader reader;

    public PenroseServerConfigReader(String filename) throws Exception {
        log.debug("Loading Penrose Server configuration: "+filename);
        init(new FileReader(filename));
    }

    public PenroseServerConfigReader(Reader reader) throws Exception {
        init(reader);
    }

    public void init(Reader reader) throws Exception {
        this.reader = reader;
        
        ClassLoader cl = getClass().getClassLoader();
        serverDtdUrl = cl.getResource("org/safehaus/penrose/config/server.dtd");
        //log.debug("Server DTD URL: "+serverDtdUrl);
    }

    public PenroseServerConfig read() throws Exception {
        PenroseServerConfig penroseServerConfig = new PenroseServerConfig();
        read(penroseServerConfig);
        return penroseServerConfig;
    }

    public void read(PenroseServerConfig penroseServerConfig) throws Exception {

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/server-digester-rules.xml");

        Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
        digester.push(penroseServerConfig);
        digester.parse(reader);
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        //log.debug("Resolving "+publicId+" "+systemId);

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);
        //log.debug("=> "+file);

        if ("server.dtd".equals(file)) {
            return new InputSource(serverDtdUrl.openStream());
        }

        return null;
    }
}
