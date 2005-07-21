/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.config;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.MappingRule;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ConfigBuilder {

    public Logger log = Logger.getLogger(Penrose.CONFIG_LOGGER);

    private Config config;

    public ConfigBuilder() {
        config = new Config();
    }

    public ConfigBuilder(Config config) {
        this.config = config;
    }

    /**
     * Load mapping configuration from a file
     *
     * @param filename the configuration file (ie. mapping.xml)
     * @throws Exception
     */
    public void loadMappingConfig(String filename) throws Exception {
        MappingRule mappingRule = new MappingRule();
        mappingRule.setFile(filename);
        loadMappingConfig(null, null, mappingRule);
    }

    /**
     * Load mapping configuration from a file
     *
     * @param dir
     * @param baseDn
     * @param mappingRule
     * @throws Exception
     */
    public void loadMappingConfig(File dir, String baseDn, MappingRule mappingRule) throws Exception {
        File file = new File(dir, mappingRule.getFile());
        log.debug("Loading mapping rule from: "+file.getAbsolutePath());

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/mapping-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(mappingRule);
		digester.parse(file);

        if (mappingRule.getBaseDn() != null) {
            baseDn = mappingRule.getBaseDn();
        }

        Collection contents = mappingRule.getContents();
        for (Iterator i=contents.iterator(); i.hasNext(); ) {
            Object object = i.next();

            if (object instanceof MappingRule) {

                MappingRule mr = (MappingRule)object;
                loadMappingConfig(file.getParentFile(), baseDn, mr);

            } else if (object instanceof EntryDefinition) {

                EntryDefinition ed = (EntryDefinition)object;
                if (ed.getDn() == null) {
                    ed.setDn(baseDn);

                } else if (baseDn != null) {
                    String parentDn = ed.getParentDn();
                    ed.setParentDn(parentDn == null ? baseDn : parentDn+","+baseDn);
                }
                config.addEntryDefinition(ed);

                Collection childDefinitions = ed.getChildDefinitions();
                for (Iterator j=childDefinitions.iterator(); j.hasNext(); ) {
                    MappingRule mr = (MappingRule)j.next();
                    loadMappingConfig(file.getParentFile(), ed.getDn(), mr);
                }
            }
        }
    }

    /**
     * Load modules configuration from a file
     *
     * @param filename the configuration file (ie. modules.xml)
     * @throws Exception
     */
    public void loadModulesConfig(String filename) throws Exception {
        if (filename == null) return;
        File file = new File(filename);
        loadModulesConfig(file);
    }

    /**
     * Load mapping configuration from a file
     *
     * @param file the configuration file (ie. modules.xml)
     * @throws Exception
     */
	public void loadModulesConfig(File file) throws Exception {
        log.debug("Loading modules configuration file from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/modules-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(config);
		digester.parse(file);
	}

    /**
     * Load server configuration from a file
     *
     * @param filename the configuration file (ie. server.xml)
     * @throws Exception
     */
    public void loadServerConfig(String filename) throws Exception {
        File file = new File(filename);
        loadServerConfig(file);
    }

	/**
	 * Load server configuration from a file
	 *
	 * @param file the configuration file (ie. server.xml)
	 * @throws Exception
	 */
	public void loadServerConfig(File file) throws Exception {
		log.debug("Loading server configuration file from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/server-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(config);
		digester.parse(file);
	}

    /**
     * Load sources configuration from a file
     *
     * @param filename the configuration file (ie. sources.xml)
     * @throws Exception
     */
    public void loadSourcesConfig(String filename) throws Exception {
        File file = new File(filename);
        loadSourcesConfig(file);
    }

	/**
	 * Load sources configuration from a file
	 *
	 * @param file the configuration file (ie. sources.xml)
	 * @throws Exception
	 */
	public void loadSourcesConfig(File file) throws Exception {
		log.debug("Loading source configuration file from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/sources-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setValidating(false);
        digester.setClassLoader(cl);
        digester.push(config);
        digester.parse(file);
	}

    public void storeServerConfig(String filename) throws Exception {
        File file = new File(filename);
        storeServerConfig(file);
    }

	/**
	 * Store configuration into xml file.
	 *
	 * @param file
	 * @throws Exception
	 */
	public void storeServerConfig(File file) throws Exception {
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
		writer.write(XMLHelper.toServerXmlElement(config));
		writer.close();
	}

    public void storeMappingConfig(String filename) throws Exception {
        File file = new File(filename);
        storeMappingConfig(file);
    }

	public void storeMappingConfig(File file) throws Exception {
		FileWriter fw = new FileWriter(file);
		OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

		XMLWriter writer = new XMLWriter(fw, format);
		writer.startDocument();
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
		writer.write(XMLHelper.toMappingXmlElement(config));
		writer.close();
	}

    public void storeSourcesConfig(File file) throws Exception {
		FileWriter fw = new FileWriter(file);
		OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

		XMLWriter writer = new XMLWriter(fw, format);
		writer.startDocument();
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
		writer.write(XMLHelper.toSourcesXmlElement(config));
		writer.close();
    }

    public void storeModulesConfig(File file) throws Exception {
		FileWriter fw = new FileWriter(file);
		OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

		XMLWriter writer = new XMLWriter(fw, format);
		writer.startDocument();
		/*
		writer.startDTD("mapping",
				"-//Penrose/Penrose Server Configuration DTD 1.0//EN",
				"http://penrose.safehaus.org/dtd/penrose-mapping-config-1.0.dtd");
				*/
		writer.write(XMLHelper.toModulesXmlElement(config));
		writer.close();
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

}
