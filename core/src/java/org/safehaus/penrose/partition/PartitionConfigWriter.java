package org.safehaus.penrose.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.cache.CacheConfig;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PartitionConfigWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    public PartitionConfigWriter() {
    }

    public void write(String directory, PartitionConfig partitionConfig) throws Exception {
        File dir = new File(directory);
        dir.mkdirs();

        File file = new File(directory, "partition.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "partition",
                "-//Penrose/DTD Partition 1.2//EN",
                "http://penrose.safehaus.org/dtd/partition.dtd"
        );

        writer.write(toElement(partitionConfig));
        writer.close();
    }

    public Element toElement(PartitionConfig partitionConfig) throws Exception {
        Element element = new DefaultElement("partition");
        if (!partitionConfig.isEnabled()) element.add(new DefaultAttribute("enabled", "false"));

        Element name = new DefaultElement("name");
        name.add(new DefaultText(partitionConfig.getName()));
        element.add(name);

        if (partitionConfig.getDescription() != null) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(partitionConfig.getName()));
            element.add(description);
        }

        if (partitionConfig.getEntryCacheConfig() != null) {
            Element entryCache = new DefaultElement("entry-cache");
            addElements(entryCache, partitionConfig.getEntryCacheConfig());
            element.add(entryCache);
        }

        if (partitionConfig.getSourceCacheConfig() != null) {
            Element sourceCache = new DefaultElement("source-cache");
            addElements(sourceCache, partitionConfig.getSourceCacheConfig());
            element.add(sourceCache);
        }

        return element;
    }

    public void addElements(Element element, CacheConfig cacheConfig) {

        //element.add(new DefaultAttribute("name", cacheConfig.getName()));

        if (cacheConfig.getCacheClass() != null && !"".equals(cacheConfig.getCacheClass())) {
            Element cacheClass = new DefaultElement("cache-class");
            cacheClass.add(new DefaultText(cacheConfig.getCacheClass()));
            element.add(cacheClass);
        }

        if (cacheConfig.getDescription() != null && !"".equals(cacheConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(cacheConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = cacheConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)cacheConfig.getParameter(name);

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }
    }
}
