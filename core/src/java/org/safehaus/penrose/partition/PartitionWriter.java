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
package org.safehaus.penrose.partition;

import java.io.File;
import java.io.FileWriter;

import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.module.ModuleWriter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.handler.HandlerConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.connection.ConnectionWriter;
import org.safehaus.penrose.source.SourceWriter;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PartitionWriter {

    public Logger log = LoggerFactory.getLogger(getClass());

    ConnectionWriter connectionWriter = new ConnectionWriter();
    SourceWriter sourceWriter = new SourceWriter();
    MappingWriter mappingWriter = new MappingWriter();
    ModuleWriter moduleWriter = new ModuleWriter();

    public PartitionWriter() {
    }

    public void write(File directory, PartitionConfig partitionConfig) throws Exception {
        File dirInf = new File(directory, "DIR-INF");
        dirInf.mkdirs();

        writePartitionXml(dirInf, partitionConfig);

        File connectionsXml = new File(dirInf, "connections.xml");
        connectionWriter.write(connectionsXml, partitionConfig.getConnectionConfigs());

        File sourcesXml = new File(dirInf, "sources.xml");
        sourceWriter.write(sourcesXml, partitionConfig.getSourceConfigs());

        File mappingXml = new File(dirInf, "mapping.xml");
        mappingWriter.write(mappingXml, partitionConfig.getDirectoryConfigs());

        File modulesXml = new File(dirInf, "modules.xml");
        moduleWriter.write(modulesXml, partitionConfig.getModuleConfigs());
    }

    public void writePartitionXml(File directory, PartitionConfig partitionConfig) throws Exception {
        File file = new File(directory, "partition.xml");

        log.debug("Writing "+file+".");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "partition",
                "-//Penrose/DTD Partition "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/partition.dtd"
        );

        writer.write(createElement(partitionConfig));
        writer.close();
    }

    public Element createElement(PartitionConfig partitionConfig)  {
        Element element = new DefaultElement("partition");

        if (!partitionConfig.isEnabled()) element.addAttribute("enabled", "false");

        String s = partitionConfig.getDescription();
        if (s != null && !"".equals(s)) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(s));
            element.add(description);
        }

        for (InterpreterConfig interpreterConfig : partitionConfig.getInterpreterConfigs()) {
            element.add(createElement(interpreterConfig));
        }

        for (EngineConfig engineConfig : partitionConfig.getEngineConfigs()) {
            element.add(createElement(engineConfig));
        }

        for (HandlerConfig handlerConfig : partitionConfig.getHandlerConfigs()) {
            element.add(createElement(handlerConfig));
        }

        for (AdapterConfig adapterConfig : partitionConfig.getAdapterConfigs()) {
            element.add(createElement(adapterConfig));
        }

        return element;
    }

    public Element createElement(InterpreterConfig interpreterConfig) {
        Element element = new DefaultElement("interpreter");
/*
        Element interpreterName = new DefaultElement("interpreter-name");
        interpreterName.add(new DefaultText(interpreterConfig.getName()));
        element.add(interpreterName);
*/
        Element interpreterClass = new DefaultElement("interpreter-class");
        interpreterClass.add(new DefaultText(interpreterConfig.getInterpreterClass()));
        element.add(interpreterClass);

        if (interpreterConfig.getDescription() != null && !"".equals(interpreterConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(interpreterConfig.getDescription()));
            element.add(description);
        }

        for (String name : interpreterConfig.getParameterNames()) {
            String value = interpreterConfig.getParameter(name);

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

    public Element createElement(EngineConfig engineConfig) {
        Element element = new DefaultElement("engine");
        element.addAttribute("name", engineConfig.getName());

        Element engineClass = new DefaultElement("engine-class");
        engineClass.add(new DefaultText(engineConfig.getEngineClass()));
        element.add(engineClass);

        if (engineConfig.getDescription() != null && !"".equals(engineConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(engineConfig.getDescription()));
            element.add(description);
        }

        for (String name : engineConfig.getParameterNames()) {
            String value = engineConfig.getParameter(name);

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

    public Element createElement(HandlerConfig handlerConfig) {
        Element element = new DefaultElement("handler");
        element.addAttribute("name", handlerConfig.getName());

        Element handlerClass = new DefaultElement("handler-class");
        handlerClass.add(new DefaultText(handlerConfig.getHandlerClass()));
        element.add(handlerClass);

        if (handlerConfig.getDescription() != null && !"".equals(handlerConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(handlerConfig.getDescription()));
            element.add(description);
        }

        for (String name : handlerConfig.getParameterNames()) {
            String value = handlerConfig.getParameter(name);

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

    public Element createElement(AdapterConfig adapterConfig) {
        Element element = new DefaultElement("adapter");
        element.addAttribute("name", adapterConfig.getName());

        Element adapterClass = new DefaultElement("adapter-class");
        adapterClass.add(new DefaultText(adapterConfig.getAdapterClass()));
        element.add(adapterClass);

        if (adapterConfig.getDescription() != null && !"".equals(adapterConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(adapterConfig.getDescription()));
            element.add(description);
        }

        for (String name : adapterConfig.getParameterNames()) {
            String value = adapterConfig.getParameter(name);

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
