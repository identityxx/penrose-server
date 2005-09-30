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
		writer.write(toServerXmlElement());
		writer.close();
	}

	public Element toServerXmlElement() {
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

		// caches
        for (Iterator iter = serverConfig.getCacheConfigs().iterator(); iter.hasNext();) {
            CacheConfig cacheConfig = (CacheConfig)iter.next();
            element.add(toElement(cacheConfig));
        }

        // adapters
        for (Iterator iter = serverConfig.getAdapterConfigs().iterator(); iter.hasNext();) {
            AdapterConfig adapter = (AdapterConfig)iter.next();
            element.add(toElement(adapter));
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

    public Element toElement(AdapterConfig adapter) {
        Element element = new DefaultElement("adapter");

        Element adapterName = new DefaultElement("adapter-name");
        adapterName.add(new DefaultText(adapter.getAdapterName()));
        element.add(adapterName);

        Element adapterClass = new DefaultElement("adapter-class");
        adapterClass.add(new DefaultText(adapter.getAdapterClass()));
        element.add(adapterClass);

        if (adapter.getDescription() != null) { 
            Element description = new DefaultElement("description");
            description.add(new DefaultText(adapter.getDescription()));
            element.add(description);
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

        // parameters
        for (Iterator iter = interpreterConfig.getParameterNames().iterator(); iter.hasNext();) {
            String name = (String) iter.next();
            String value = (String) interpreterConfig.getParameter(name);

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

        Element interpreterName = new DefaultElement("engine-name");
        interpreterName.add(new DefaultText(engineConfig.getEngineName()));
        element.add(interpreterName);

        Element interpreterClass = new DefaultElement("engine-class");
        interpreterClass.add(new DefaultText(engineConfig.getEngineClass()));
        element.add(interpreterClass);

        return element;
    }

    public Element toElement(CacheConfig cache) {
    	Element element = new DefaultElement("cache");

        Element cacheName = new DefaultElement("cache-name");
        cacheName.add(new DefaultText(cache.getCacheName()));
        element.add(cacheName);

        if (cache.getCacheClass() != null && !"".equals(cache.getCacheClass())) {
            Element cacheClass = new DefaultElement("cache-class");
            cacheClass.add(new DefaultText(cache.getCacheClass()));
            element.add(cacheClass);
        }

        if (cache.getDescription() != null && !"".equals(cache.getDescription())) {
            Element description = new DefaultElement("description");
            description.add(new DefaultText(cache.getDescription()));
            element.add(description);
        }

        for (Iterator iter = cache.getParameterNames().iterator(); iter.hasNext();) {
            String name = (String) iter.next();
            String value = (String) cache.getParameter(name);

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

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }
}
