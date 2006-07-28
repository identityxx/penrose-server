package org.safehaus.penrose.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.service.ServiceConfig;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;

import java.io.File;
import java.io.Writer;
import java.io.FileWriter;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseServerConfigWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    File file;

    public PenroseServerConfigWriter(String filename) throws Exception {
        file = new File(filename);
    }

    /**
     * Store configuration into xml file.
     *
     * @throws Exception
     */
    public void write(PenroseServerConfig penroseServerConfig) throws Exception {

        file.getParentFile().mkdirs();
        Writer writer = new FileWriter(file);

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter xmlWriter = new XMLWriter(writer, format);
        xmlWriter.startDocument();

        xmlWriter.startDTD(
                "server",
                "-//Penrose/DTD Server 1.0//EN",
                "http://penrose.safehaus.org/dtd/server.dtd"
        );

        xmlWriter.write(toElement(penroseServerConfig));
        xmlWriter.close();

        writer.close();
    }

    public Element toElement(PenroseServerConfig penroseServerConfig) {
        Element element = new DefaultElement("server");

        for (Iterator i = penroseServerConfig.getSystemPropertyNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = penroseServerConfig.getSystemProperty(name);

            Element parameter = new DefaultElement("system-property");

            Element paramName = new DefaultElement("property-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("property-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }

        for (Iterator i = penroseServerConfig.getServiceConfigs().iterator(); i.hasNext();) {
            ServiceConfig serviceConfig = (ServiceConfig)i.next();
            element.add(toElement(serviceConfig));
        }

        return element;
    }

    public Element toElement(ServiceConfig serviceConfig) {

        Element element = new DefaultElement("service");
        element.addAttribute("name", serviceConfig.getName());
        if (!serviceConfig.isEnabled()) element.addAttribute("enabled", "false");

        Element adapterClass = new DefaultElement("service-class");
        adapterClass.add(new DefaultText(serviceConfig.getServiceClass()));
        element.add(adapterClass);

        if (serviceConfig.getDescription() != null && !"".equals(serviceConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(serviceConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = serviceConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)serviceConfig.getParameter(name);

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
}
