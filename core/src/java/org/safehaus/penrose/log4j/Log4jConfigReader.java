/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.log4j;

import org.w3c.dom.*;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.net.URL;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfigReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    URL log4jDtdUrl;
    Reader reader;

    public Log4jConfigReader(File file) throws Exception {
        this(new FileReader(file));
    }

    public Log4jConfigReader(Reader reader) throws Exception {
        this.reader = reader;

        ClassLoader cl = getClass().getClassLoader();
        log4jDtdUrl = cl.getResource("org/apache/log4j/xml/log4j.dtd");
        log.debug("Log4j DTD URL: "+log4jDtdUrl);
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        log.debug("Resolving "+publicId+" "+systemId);

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);
        //log.debug("=> "+file);

        if ("log4j.dtd".equals(file)) {
            return new InputSource(log4jDtdUrl.openStream());
        }

        return null;
    }

    public Log4jConfig read() throws Exception {

        Log4jConfig config = new Log4jConfig();

        ClassLoader cl = getClass().getClassLoader();
        URL ruleUrl = cl.getResource("org/safehaus/penrose/log4j/log4j-digester-rules.xml");

        Digester digester = DigesterLoader.createDigester(ruleUrl);
        digester.setEntityResolver(this);
/*
        digester.register(
                "http://logging.apache.org/log4j/docs/api/org/apache/log4j/xml/log4j.dtd",
                log4jDtdUrl.toString()
        );

        digester.register(
                "log4j.dtd",
                log4jDtdUrl.toString()
        );
*/
        digester.setValidating(false);
        digester.setClassLoader(cl);
        digester.push(config);
        digester.parse(reader);

        return config;
/*
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(false);

        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(is);

        return createLog4jConfig(document);
*/
    }

    public void close() throws Exception {
        reader.close();
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
