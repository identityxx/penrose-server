package org.safehaus.penrose.partition;

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
 * @author Endi S. Dewata
 */
public class PartitionConfigReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    URL partitionDtdUrl;

    public PartitionConfigReader() {
        ClassLoader cl = getClass().getClassLoader();

        partitionDtdUrl = cl.getResource("org/safehaus/penrose/partition/partition.dtd");
    }

    public PartitionConfig read(String path) throws Exception {

        PartitionConfig partitionConfig = new PartitionConfig();

        String filename = (path == null ? "" : path+ File.separator)+"partition.xml";
        log.debug("Loading "+filename);

        File pathFile = new File(path);
        File file = new File(filename);
        if (!file.exists()) {
            log.debug("File "+filename+" not found");

            partitionConfig.setName(pathFile.getName());
            return partitionConfig;
        }

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/partition/partition-digester-rules.xml");
        Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
        digester.push(partitionConfig);
        digester.parse(file);

        return partitionConfig;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);

        URL url = null;

        if ("partition.dtd".equals(file)) {
            url = partitionDtdUrl;
        }

        if (url == null) return null;

        return new InputSource(url.openStream());
    }
}
