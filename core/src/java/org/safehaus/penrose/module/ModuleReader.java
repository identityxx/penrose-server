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

    String home;

    public ModuleReader(String home) {
        this.home = home;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public void read(String path, Modules modules) throws Exception {
        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+ File.separator+path;
        }
        String filename = (path == null ? "" : path+File.separator)+"modules.xml";
        log.debug("Loading "+filename);

        File file = new File(filename);
        if (!file.exists()) return;

        //log.debug("Loading modules configuration from: "+file.getAbsolutePath());

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
