package org.safehaus.penrose.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SourceWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    public SourceWriter() {
    }

    public void write(String directory, Partition partition) throws Exception {
        File dir = new File(directory);
        dir.mkdirs();

        File file = new File(dir, "sources.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "sources",
                "-//Penrose/DTD Sources 1.2//EN",
                "http://penrose.safehaus.org/dtd/sources.dtd"
        );

        writer.write(toElement(partition));
        writer.close();
    }

    public Element toElement(Partition partition) {
        Element element = new DefaultElement("sources");

        for (Iterator i = partition.getSourceConfigs().iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();
            element.add(toElement(sourceConfig));
        }

        return element;
    }

    public Element toElement(SourceConfig source) {
        Element element = new DefaultElement("source");
        element.addAttribute("name", source.getName());

        Element adapterName = new DefaultElement("connection-name");
        adapterName.add(new DefaultText(source.getConnectionName()));
        element.add(adapterName);

        for (Iterator i = source.getFieldConfigs().iterator(); i.hasNext(); ) {
            FieldConfig field = (FieldConfig)i.next();
            Element fieldElement = toElement(field);
            element.add(fieldElement);
        }

        for (Iterator i = source.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = source.getParameter(name);
            if ("".equals(value)) continue;

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }

        return element;
    }

    public Element toElement(FieldConfig field) {
        Element element = new DefaultElement("field");
        element.addAttribute("name", field.getName());
        if (!field.getName().equals(field.getOriginalName())) element.addAttribute("originalName", field.getOriginalName());
        if (field.isPK()) element.addAttribute("primaryKey", "true");
        if (!field.isSearchable()) element.addAttribute("searchable", "false");
        if (field.isUnique()) element.addAttribute("unique", "true");
        if (field.isIndex()) element.addAttribute("index", "true");
        if (field.isCaseSensitive()) element.addAttribute("caseSensitive", "true");
        if (!FieldConfig.DEFAULT_TYPE.equals(field.getType())) element.addAttribute("type", field.getType());
        if (field.getLength() != FieldConfig.DEFAULT_LENGTH) element.addAttribute("length", ""+field.getLength());
        if (field.getPrecision() != FieldConfig.DEFAULT_PRECISION) element.addAttribute("precision", ""+field.getPrecision());
        return element;
    }
}
