package org.safehaus.penrose.log4j;

import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfigReader {

    InputStream is;

    public Log4jConfigReader(File file) throws Exception {
        is = new FileInputStream(file);
    }

    public Log4jConfig read() throws Exception {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false);

        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(is);

        return createLog4jConfig(document);
    }

    public void close() throws Exception {
        is.close();
    }
    
    public Log4jConfig createLog4jConfig(Document document) {

        Log4jConfig config = new Log4jConfig();

        NodeList appenders = document.getElementsByTagName("appender");

        for (int i=0; i<appenders.getLength(); i++) {
            Element appenderElement = (Element)appenders.item(i);
            AppenderConfig appenderConfig = createAppenderConfig(appenderElement);
            config.addAppenderConfig(appenderConfig);
        }

        NodeList loggers = document.getElementsByTagName("logger");

        for (int i=0; i<loggers.getLength(); i++) {
            Element loggerElement = (Element)loggers.item(i);
            LoggerConfig loggerConfig = createLoggerConfig(loggerElement);
            config.addLoggerConfig(loggerConfig);
        }

        NodeList roots = document.getElementsByTagName("root");

        if (roots.getLength() > 0) {
            Element rootElement = (Element)roots.item(0);
            RootConfig rootConfig = createRootConfig(rootElement);
            config.setRootConfig(rootConfig);
        }

        return config;
    }

    public AppenderConfig createAppenderConfig(Element element) {

        AppenderConfig appenderConfig = new AppenderConfig();

        appenderConfig.setName(element.getAttribute("name"));
        appenderConfig.setAppenderClass(element.getAttribute("class"));

        NodeList params = element.getElementsByTagName("param");

        for (int i=0; i<params.getLength(); i++) {
            Element paramElement = (Element)params.item(i);
            String name = paramElement.getAttribute("name");
            String value = paramElement.getAttribute("value");
            appenderConfig.setParameter(name, value);
        }

        NodeList layouts = element.getElementsByTagName("layout");

        if (layouts.getLength() > 0) {
            Element layoutElement = (Element)layouts.item(0);
            LayoutConfig layoutConfig = createLayoutConfig(layoutElement);

            appenderConfig.setLayoutConfig(layoutConfig);
        }

        return appenderConfig;
    }

    public LayoutConfig createLayoutConfig(Element element) {

        LayoutConfig layoutConfig = new LayoutConfig();

        layoutConfig.setLayoutClass(element.getAttribute("class"));

        NodeList params = element.getElementsByTagName("param");

        for (int i=0; i<params.getLength(); i++) {
            Element paramElement = (Element)params.item(i);
            String name = paramElement.getAttribute("name");
            String value = paramElement.getAttribute("value");
            layoutConfig.setParameter(name, value);
        }

        return layoutConfig;
    }

    public LoggerConfig createLoggerConfig(Element element) {

        LoggerConfig loggerConfig = new LoggerConfig();

        loggerConfig.setName(element.getAttribute("name"));
        loggerConfig.setAdditivity(new Boolean(element.getAttribute("additivity")).booleanValue());

        NodeList levels = element.getElementsByTagName("level");

        if (levels.getLength() > 0) {
            Element levelElement = (Element)levels.item(0);
            String level = levelElement.getAttribute("value");
            loggerConfig.setLevel(level);
        }

        NodeList appenderRefs = element.getElementsByTagName("appender-ref");

        for (int i=0; i<appenderRefs.getLength(); i++) {
            Element appenderRef = (Element)appenderRefs.item(i);
            String appenderName = appenderRef.getAttribute("ref");
            loggerConfig.addAppender(appenderName);
        }

        return loggerConfig;
    }

    public RootConfig createRootConfig(Element element) {

        RootConfig rootConfig = new RootConfig();

        NodeList levels = element.getElementsByTagName("level");

        if (levels.getLength() > 0) {
            Element levelElement = (Element)levels.item(0);
            String level = levelElement.getAttribute("value");
            rootConfig.setLevel(level);
        }

        NodeList appenderRefs = element.getElementsByTagName("appender-ref");

        for (int i=0; i<appenderRefs.getLength(); i++) {
            Element appenderRef = (Element)appenderRefs.item(i);
            String appenderName = appenderRef.getAttribute("ref");
            rootConfig.addAppender(appenderName);
        }

        return rootConfig;
    }

    public void print(Document document) {
        Element element = document.getDocumentElement();
        print(0, element);
    }

    public void print(int level, Element node) {
        for (int i=0; i<level; i++) {
            System.out.print("  ");
        }

        if (node instanceof Text) {
            System.out.println("Text: "+node.getNodeValue());

        } else {
            System.out.println(node.getNodeName()+": "+node.getNodeValue());
        }

        NamedNodeMap map = node.getAttributes();
        for (int i=0; map != null && i<map.getLength(); i++) {
            Node n = map.item(i);
            System.out.println(" - "+n.getNodeName()+": "+n.getNodeValue());
        }

        NodeList list = node.getChildNodes();
        for (int i=0; i<list.getLength(); i++) {
            print(level+1, (Element)list.item(i));
        }
    }
}
