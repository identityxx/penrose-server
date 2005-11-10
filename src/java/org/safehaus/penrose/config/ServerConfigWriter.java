/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose.config;

import java.util.Iterator;
import java.io.File;
import java.io.FileWriter;

import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultText;
import org.safehaus.penrose.connection.AdapterConfig;
import org.safehaus.penrose.cache.CacheConfig;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.connector.ConnectorConfig;

/**
 * @author Endi S. Dewata
 */
public class ServerConfigWriter {

    private ServerConfig serverConfig;

    public ServerConfigWriter(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public void write(String filename) throws Exception {
        File file = new File(filename);
        write(file);
    }

	/**
	 * Store configuration into xml file.
	 *
	 * @param file
	 * @throws Exception
	 */
	public void write(File file) throws Exception {
		FileWriter fw = new FileWriter(file);
		OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

		XMLWriter writer = new XMLWriter(fw, format);
		writer.startDocument();
		/*
		writer.startDTD("server",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-server-config-1.0.dtd");
				*/
		writer.write(toElement());
		writer.close();
	}

	public Element toElement() {
		Element element = new DefaultElement("server");

        // interpreters
        for (Iterator iter = serverConfig.getInterpreterConfigs().iterator(); iter.hasNext();) {
            InterpreterConfig interpreterConfig = (InterpreterConfig)iter.next();
            element.add(toElement(interpreterConfig));
        }

        // engines
        for (Iterator iter = serverConfig.getEngineConfigs().iterator(); iter.hasNext();) {
            EngineConfig engineConfig = (EngineConfig)iter.next();
            element.add(toElement(engineConfig));
        }

        // connectors
        for (Iterator iter = serverConfig.getConnectorConfigs().iterator(); iter.hasNext();) {
            ConnectorConfig connectorConfig = (ConnectorConfig)iter.next();
            element.add(toElement(connectorConfig));
        }

        // adapters
        for (Iterator iter = serverConfig.getAdapterConfigs().iterator(); iter.hasNext();) {
            AdapterConfig adapterConfig = (AdapterConfig)iter.next();
            element.add(toElement(adapterConfig));
        }

		// root
        if (serverConfig.getRootDn() != null || serverConfig.getRootPassword() != null) {
            Element rootElement = new DefaultElement("root");

            if (serverConfig.getRootDn() != null) {
                Element rootDn = new DefaultElement("root-dn");
                rootDn.add(new DefaultText(serverConfig.getRootDn()));
                rootElement.add(rootDn);
            }

            if (serverConfig.getRootPassword() != null) {
                Element rootPassword = new DefaultElement("root-password");
                rootPassword.add(new DefaultText(serverConfig.getRootPassword()));
                rootElement.add(rootPassword);
            }

            element.add(rootElement);
        }

		return element;
	}

    public Element toElement(AdapterConfig adapterConfig) {
        Element element = new DefaultElement("adapter");

        Element adapterName = new DefaultElement("adapter-name");
        adapterName.add(new DefaultText(adapterConfig.getAdapterName()));
        element.add(adapterName);

        Element adapterClass = new DefaultElement("adapter-class");
        adapterClass.add(new DefaultText(adapterConfig.getAdapterClass()));
        element.add(adapterClass);

        if (adapterConfig.getDescription() != null && !"".equals(adapterConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(adapterConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = adapterConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)adapterConfig.getParameter(name);

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

    public Element toElement(InterpreterConfig interpreterConfig) {
    	Element element = new DefaultElement("interpreter");

        Element interpreterName = new DefaultElement("interpreter-name");
        interpreterName.add(new DefaultText(interpreterConfig.getInterpreterName()));
        element.add(interpreterName);

        Element interpreterClass = new DefaultElement("interpreter-class");
        interpreterClass.add(new DefaultText(interpreterConfig.getInterpreterClass()));
        element.add(interpreterClass);

        if (interpreterConfig.getDescription() != null && !"".equals(interpreterConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(interpreterConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = interpreterConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)interpreterConfig.getParameter(name);

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

    public Element toElement(EngineConfig engineConfig) {
    	Element element = new DefaultElement("engine");

        Element engineName = new DefaultElement("engine-name");
        engineName.add(new DefaultText(engineConfig.getEngineName()));
        element.add(engineName);

        Element engineClass = new DefaultElement("engine-class");
        engineClass.add(new DefaultText(engineConfig.getEngineClass()));
        element.add(engineClass);

        if (engineConfig.getDescription() != null && !"".equals(engineConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(engineConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = engineConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)engineConfig.getParameter(name);

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }

        for (Iterator iter = engineConfig.getCacheConfigs().iterator(); iter.hasNext();) {
            CacheConfig cacheConfig = (CacheConfig)iter.next();
            element.add(toElement(cacheConfig));
        }

        return element;
    }

    public Element toElement(CacheConfig cacheConfig) {
    	Element element = new DefaultElement("cache");

        Element cacheName = new DefaultElement("cache-name");
        cacheName.add(new DefaultText(cacheConfig.getCacheName()));
        element.add(cacheName);

        if (cacheConfig.getCacheClass() != null && !"".equals(cacheConfig.getCacheClass())) {
            Element cacheClass = new DefaultElement("cache-class");
            cacheClass.add(new DefaultText(cacheConfig.getCacheClass()));
            element.add(cacheClass);
        }

        if (cacheConfig.getDescription() != null && !"".equals(cacheConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(cacheConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = cacheConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)cacheConfig.getParameter(name);

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

    public Element toElement(ConnectorConfig connectorConfig) {
    	Element element = new DefaultElement("connector");

        Element cacheName = new DefaultElement("connector-name");
        cacheName.add(new DefaultText(connectorConfig.getConnectorName()));
        element.add(cacheName);

        if (connectorConfig.getConnectorClass() != null && !"".equals(connectorConfig.getConnectorClass())) {
            Element cacheClass = new DefaultElement("connector-class");
            cacheClass.add(new DefaultText(connectorConfig.getConnectorClass()));
            element.add(cacheClass);
        }

        if (connectorConfig.getDescription() != null && !"".equals(connectorConfig.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(connectorConfig.getDescription()));
            element.add(description);
        }

        for (Iterator i = connectorConfig.getParameterNames().iterator(); i.hasNext();) {
            String name = (String)i.next();
            String value = (String)connectorConfig.getParameter(name);

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            element.add(parameter);
        }

        for (Iterator iter = connectorConfig.getCacheConfigs().iterator(); iter.hasNext();) {
            CacheConfig cacheConfig = (CacheConfig)iter.next();
            element.add(toElement(cacheConfig));
        }

    	return element;
    }

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }
}
