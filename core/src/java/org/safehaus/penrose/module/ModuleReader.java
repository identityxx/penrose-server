package org.safehaus.penrose.module;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;

import java.net.URL;
import java.io.File;
import java.io.IOException;

/**
 * @author Endi S. Dewata
 */
public class ModuleReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;

    public ModuleReader() {
        ClassLoader cl = getClass().getClassLoader();
        dtdUrl = cl.getResource("org/safehaus/penrose/module/modules.dtd");
    }

    public void read(String path, Partition partition) throws Exception {
        String filename = (path == null ? "" : path+File.separator)+"modules.xml";
        log.debug("Loading "+filename);

        File file = new File(filename);
        if (!file.exists()) {
            log.debug("File "+filename+" not found");
            return;
        }

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/module/modules-digester-rules.xml");
        Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
        digester.push(partition);
        digester.parse(file);
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);

        URL url = null;

        if ("modules.dtd".equals(file)) {
            url = dtdUrl;
        }

        if (url == null) return null;

        return new InputSource(url.openStream());
    }
}
