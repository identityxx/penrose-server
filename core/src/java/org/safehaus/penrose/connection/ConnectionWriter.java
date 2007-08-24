package org.safehaus.penrose.connection;

import org.safehaus.penrose.Penrose;
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

    public void write(File file, ConnectionConfigs connectionConfigs) throws Exception {

        log.debug("Writing "+file+".");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "connections",
                "-//Penrose/DTD Connections "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/connections.dtd"
        );

        writer.write(createElement(connectionConfigs));
        writer.close();
    }

    public Element createElement(ConnectionConfigs connectionConfigs) {
        Element element = new DefaultElement("connections");

        for (ConnectionConfig connectionConfig : connectionConfigs.getConnectionConfigs()) {
            element.add(createElement(connectionConfig));
        }

        return element;
    }

    public Element createElement(ConnectionConfig connectionConfig) {
        Element element = new DefaultElement("connection");
        element.add(new DefaultAttribute("name", connectionConfig.getName()));

        Element adapterName = new DefaultElement("adapter-name");
        adapterName.add(new DefaultText(connectionConfig.getAdapterName()));
        element.add(adapterName);

        // parameters
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
