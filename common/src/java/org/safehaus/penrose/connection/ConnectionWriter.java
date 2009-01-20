package org.safehaus.penrose.connection;

import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;

/**
 * @author Endi Sukma Dewata
 */
public class ConnectionWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    public ConnectionWriter() {
    }

    public void write(File file, ConnectionConfigManager connectionConfigManager) throws Exception {

        log.debug("Writing "+file+".");

        Element element = createElement(connectionConfigManager);

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "connections",
                "-//Penrose/DTD Connections "+getClass().getPackage().getSpecificationVersion()+"//EN",
                "http://penrose.safehaus.org/dtd/connections.dtd"
        );

        writer.write(element);
        writer.close();
    }

    public Element createElement(ConnectionConfigManager connectionConfigManager) {

        Element element = new DefaultElement("connections");

        for (ConnectionConfig connectionConfig : connectionConfigManager.getConnectionConfigs()) {
            element.add(createElement(connectionConfig));
        }

        return element;
    }

    public Element createElement(ConnectionConfig connectionConfig) {

        Element element = new DefaultElement("connection");
        element.add(new DefaultAttribute("name", connectionConfig.getName()));
        if (!connectionConfig.isEnabled()) element.addAttribute("enabled", "false");

        String connectionClass = connectionConfig.getConnectionClass();
        if (connectionClass != null) {
            Element descriptionElement = new DefaultElement("connection-class");
            descriptionElement.add(new DefaultText(connectionClass));
            element.add(descriptionElement);
        }

        String description = connectionConfig.getDescription();
        if (description != null) {
            Element descriptionElement = new DefaultElement("description");
            descriptionElement.add(new DefaultText(description));
            element.add(descriptionElement);
        }

        String adapterName = connectionConfig.getAdapterName();
        if (adapterName != null) {
            Element adapterElement = new DefaultElement("adapter-name");
            adapterElement.add(new DefaultText(adapterName));
            element.add(adapterElement);
        }

        for (String name : connectionConfig.getParameterNames()) {
            String value = connectionConfig.getParameter(name);

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
