package org.safehaus.penrose.module;

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
public class ModuleReader implements EntityResolver {

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public ModuleReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/module/modules.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/module/modules-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public void read(File file, ModuleConfigManager modules) throws Exception {

        if (!file.exists()) return;

		digester.push(modules);
		digester.parse(file);
        digester.pop();
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        return new InputSource(dtdUrl.openStream());
    }
}
