package org.safehaus.penrose.log4j;

import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.Element;

import java.io.Writer;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class Log4jConfigWriter {

    Writer out;

    public Log4jConfigWriter() {
        out = new PrintWriter(System.out, true);
    }

    public Log4jConfigWriter(File file) throws Exception {
        out = new FileWriter(file);
    }

    public void write(Log4jConfig config) throws Exception {
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(out, format);
        writer.startDocument();

        writer.startDTD(
                "log4j:configuration",
                "-//Apache//DTD Log4j 1.2//EN",
                "http://logging.apache.org/log4j/docs/api/org/apache/log4j/xml/log4j.dtd");

        writer.write(createConfigElement(config));
        writer.close();
    }

    public void close() throws Exception {
        out.close();
    }

    public Element createConfigElement(Log4jConfig config) {

        Element element = new DefaultElement("log4j:configuration");

        element.addAttribute("xmlns:log4j", "http://jakarta.apache.org/log4j/");
        if (config.isDebug()) element.addAttribute("debug", "true");

        for (Iterator i=config.getAppenderConfigs().iterator(); i.hasNext(); ) {
            AppenderConfig appenderConfig = (AppenderConfig)i.next();
            Element appenderElement = createAppenderElement(appenderConfig);
            element.add(appenderElement);
        }

        for (Iterator i=config.getLoggerConfigs().iterator(); i.hasNext(); ) {
            LoggerConfig loggerConfig = (LoggerConfig)i.next();
            Element loggerElement = createLoggerElement(loggerConfig);
            element.add(loggerElement);
        }

        if (config.getRootConfig() != null) {
            Element rootElement = createRootElement(config.getRootConfig());
            element.add(rootElement);
        }

        return element;
    }

    public Element createAppenderElement(AppenderConfig appenderConfig) {

        Element element = new DefaultElement("appender");

        element.addAttribute("name", appenderConfig.getName());
        element.addAttribute("class", appenderConfig.getAppenderClass());

        if (appenderConfig.getLayoutConfig() != null) {
            Element layoutElement = createLayoutElement(appenderConfig.getLayoutConfig());
            element.add(layoutElement);
        }

        return element;
    }

    public Element createLayoutElement(LayoutConfig layoutConfig) {

        Element element = new DefaultElement("layout");

        element.addAttribute("class", layoutConfig.getLayoutClass());

        for (Iterator i=layoutConfig.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
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
        if (!loggerConfig.isAdditivity()) element.addAttribute("additivity", "false");

        Element levelElement = new DefaultElement("level");
        levelElement.addAttribute("value", loggerConfig.getLevel());
        element.add(levelElement);

        for (Iterator i=loggerConfig.getAppenders().iterator(); i.hasNext(); ) {
            String appenderName = (String)i.next();

            Element appenderRefElement = new DefaultElement("appender-ref");
            appenderRefElement.addAttribute("ref", appenderName);

            element.add(appenderRefElement);
        }

        return element;
    }

    public Element createRootElement(RootConfig rootConfig) {

        Element element = new DefaultElement("root");

        Element levelElement = new DefaultElement("level");
        levelElement.addAttribute("value", rootConfig.getLevel());
        element.add(levelElement);

        for (Iterator i=rootConfig.getAppenders().iterator(); i.hasNext(); ) {
            String appenderName = (String)i.next();

            Element appenderRefElement = new DefaultElement("appender-ref");
            appenderRefElement.addAttribute("ref", appenderName);

            element.add(appenderRefElement);
        }

        return element;
    }
}
