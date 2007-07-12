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

    Logger log = LoggerFactory.getLogger(getClass());

    public ModuleReader() {
    }

    public void read(String filename, Modules modules) throws Exception {
        File file = new File(filename);
        if (!file.exists()) return;

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/module/modules-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
		digester.push(modules);
		digester.parse(file);
	}

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/module/modules.dtd");
        return new InputSource(url.openStream());
    }
}
