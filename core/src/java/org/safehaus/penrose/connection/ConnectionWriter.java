package org.safehaus.penrose.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.Penrose;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultText;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ConnectionWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    public ConnectionWriter() {
    }

    public void write(String directory, Partition partition) throws Exception {
        File dir = new File(directory);
        dir.mkdirs();

        File file = new File(dir, "connections.xml");

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

        writer.write(toElement(partition));
        writer.close();
    }

    public Element toElement(Partition partition) {
        Element element = new DefaultElement("connections");

        for (Iterator i = partition.getConnectionConfigs().iterator(); i.hasNext();) {
            ConnectionConfig connection = (ConnectionConfig)i.next();
            element.add(toElement(connection));
        }

        return element;
    }

    public Element toElement(ConnectionConfig connection) {
        Element element = new DefaultElement("connection");
        element.add(new DefaultAttribute("name", connection.getName()));

        Element adapterName = new DefaultElement("adapter-name");
        adapterName.add(new DefaultText(connection.getAdapterName()));
        element.add(adapterName);

        for (Iterator iter = connection.parameters.keySet().iterator(); iter.hasNext();) {
            String name = (String) iter.next();
            String value = (String) connection.parameters.get(name);

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
