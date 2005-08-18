/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.config;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.log4j.Logger;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.MappingRule;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ConfigReader {

    public Logger log = Logger.getLogger(getClass());

    public ConfigReader() {
    }

    public Config read(String directory) throws Exception {
        Config config = new Config();
        loadSourcesConfig(directory+"/sources.xml", config);
        loadMappingConfig(directory+"/mapping.xml", config);
        loadModulesConfig(directory+"/modules.xml", config);
        return config;
    }

    /**
     * Load mapping configuration from a file
     *
     * @param filename the configuration file (ie. mapping.xml)
     * @throws Exception
     */
    public void loadMappingConfig(String filename, Config config) throws Exception {
        MappingRule mappingRule = new MappingRule();
        mappingRule.setFile(filename);
        loadMappingConfig(null, null, mappingRule, config);
    }

    /**
     * Load mapping configuration from a file
     *
     * @param dir
     * @param baseDn
     * @param mappingRule
     * @throws Exception
     */
    public void loadMappingConfig(File dir, String baseDn, MappingRule mappingRule, Config config) throws Exception {
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
                loadMappingConfig(file.getParentFile(), baseDn, mr, config);

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
                    loadMappingConfig(file.getParentFile(), ed.getDn(), mr, config);
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
    public void loadModulesConfig(String filename, Config config) throws Exception {
        if (filename == null) return;
        File file = new File(filename);
        loadModulesConfig(file, config);
    }

    /**
     * Load mapping configuration from a file
     *
     * @param file the configuration file (ie. modules.xml)
     * @throws Exception
     */
	public void loadModulesConfig(File file, Config config) throws Exception {
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
     * Load sources configuration from a file
     *
     * @param filename the configuration file (ie. sources.xml)
     * @throws Exception
     */
    public void loadSourcesConfig(String filename, Config config) throws Exception {
        File file = new File(filename);
        loadSourcesConfig(file, config);
    }

	/**
	 * Load sources configuration from a file
	 *
	 * @param file the configuration file (ie. sources.xml)
	 * @throws Exception
	 */
	public void loadSourcesConfig(File file, Config config) throws Exception {
		log.debug("Loading source configuration file from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/config/sources-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setValidating(false);
        digester.setClassLoader(cl);
        digester.push(config);
        digester.parse(file);
	}
}
