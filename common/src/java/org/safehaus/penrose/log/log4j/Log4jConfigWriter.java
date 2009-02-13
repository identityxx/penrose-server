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
package org.safehaus.penrose.log.log4j;

import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.Element;

import java.io.Writer;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfigWriter {

    public Log4jConfigWriter() {
    }

    public void write(File file, Log4jConfig config) throws Exception {

        Element element = createConfigElement(config);
        
        Writer out;
        if (file == null) {
            out = new PrintWriter(System.out, true);
        } else {
            file.getParentFile().mkdirs();
            out = new FileWriter(file);
        }

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(out, format);
        writer.startDocument();

        writer.startDTD(
                "log4j:configuration",
                "-//Apache//DTD Log4j 1.2//EN",
                "http://logging.apache.org/log4j/docs/api/org/apache/log4j/xml/log4j.dtd");

        writer.write(element);
        writer.close();

        out.close();
    }

    public void close() throws Exception {
    }

    public Element createConfigElement(Log4jConfig config) {

        Element element = new DefaultElement("log4j:configuration");

        element.addAttribute("xmlns:log4j", "http://jakarta.apache.org/log4j/");
        if (config.isDebug()) element.addAttribute("debug", "true");

        for (AppenderConfig appenderConfig : config.getAppenderConfigs()) {
            Element appenderElement = createAppenderElement(appenderConfig);
            element.add(appenderElement);
        }

        for (LoggerConfig loggerConfig : config.getLoggerConfigs()) {
            Element loggerElement = createLoggerElement(loggerConfig);
            element.add(loggerElement);
        }

        if (config.getRootLoggerConfig() != null) {
            Element rootElement = createRootElement(config.getRootLoggerConfig());
            element.add(rootElement);
        }

        return element;
    }

    public Element createAppenderElement(AppenderConfig appenderConfig) {

        Element element = new DefaultElement("appender");

        element.addAttribute("name", appenderConfig.getName());
        element.addAttribute("class", appenderConfig.getAppenderClass());

        for (String name : appenderConfig.getParameterNames()) {
            String value = appenderConfig.getParameter(name);

            Element parameterElement = createParameterElement(name, value);
            element.add(parameterElement);
        }

        if (appenderConfig.getLayoutConfig() != null) {
            Element layoutElement = createLayoutElement(appenderConfig.getLayoutConfig());
            element.add(layoutElement);
        }

        return element;
    }

    public Element createLayoutElement(LayoutConfig layoutConfig) {

        Element element = new DefaultElement("layout");

        element.addAttribute("class", layoutConfig.getLayoutClass());

        for (String name : layoutConfig.getParameterNames()) {
            String value = layoutConfig.getParameter(name);

            Element parameterElement = createParameterElement(name, value);
            element.add(parameterElement);
        }

        return element;
    }

    public Element createParameterElement(String name, String value) {

        Element element = new DefaultElement("param");

        element.addAttribute("name", name);
        element.addAttribute("value", value);

        return element;
    }

    public Element createLoggerElement(LoggerConfig loggerConfig) {

        Element element = new DefaultElement("logger");

        element.addAttribute("name", loggerConfig.getName());
        if (!loggerConfig.getAdditivity()) element.addAttribute("additivity", "false");

        if (loggerConfig.getLevel() != null) {
            Element levelElement = new DefaultElement("level");
            levelElement.addAttribute("value", loggerConfig.getLevel());
            element.add(levelElement);
        }

        for (String appenderName : loggerConfig.getAppenderNames()) {
            Element appenderRefElement = new DefaultElement("appender-ref");
            appenderRefElement.addAttribute("ref", appenderName);

            element.add(appenderRefElement);
        }

        return element;
    }

    public Element createRootElement(RootLoggerConfig rootConfig) {

        Element element = new DefaultElement("root");

        if (rootConfig.getLevel() != null) {
            Element levelElement = new DefaultElement("level");
            levelElement.addAttribute("value", rootConfig.getLevel());
            element.add(levelElement);
        }

        for (String appenderName : rootConfig.getAppenderNames()) {
            Element appenderRefElement = new DefaultElement("appender-ref");
            appenderRefElement.addAttribute("ref", appenderName);

            element.add(appenderRefElement);
        }

        return element;
    }
}
