package org.safehaus.penrose.directory;

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
public class DirectoryReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public DirectoryReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/directory/directory.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/directory/directory-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public void read(File file, DirectoryConfig directoryConfig) throws Exception {

        if (!file.exists()) return;

		digester.push(directoryConfig);
		digester.parse(file);
        digester.pop();
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
