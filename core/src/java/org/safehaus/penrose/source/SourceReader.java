package org.safehaus.penrose.source;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;

import java.io.IOException;
import java.io.File;
import java.net.URL;

/**
 * @author Endi Sukma Dewata
 */
public class SourceReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public SourceReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/source/sources.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/source/sources-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public void read(String filename, Sources sources) throws Exception {
        File file = new File(filename);
        if (!file.exists()) return;

        digester.push(sources);
        digester.parse(file);
        digester.pop();
	}

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
