package org.safehaus.penrose.mapping;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * @author Endi Sukma Dewata
 */
public class MappingReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public MappingReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/mapping/mappings.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/mapping/mappings-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public void read(File file, MappingConfigManager mappingConfigManager) throws Exception {

        if (!file.exists()) return;

		digester.push(mappingConfigManager);
		digester.parse(file);
        digester.pop();
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}