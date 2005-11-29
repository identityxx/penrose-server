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
package org.safehaus.penrose.partition;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.apache.log4j.Logger;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.interpreter.Token;
import org.safehaus.penrose.partition.Partition;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader {

    Logger log = Logger.getLogger(getClass());

    String directory;

    public PartitionReader(String directory) {
        this.directory = directory;
    }

    public Partition read() throws Exception {
        Partition partition = new Partition();
        loadConnectionsConfig(directory+File.separator+"connections.xml", partition);
        loadSourcesConfig(directory+File.separator+"sources.xml", partition);
        loadMappingConfig(directory+File.separator+"mapping.xml", partition);
        loadModulesConfig(directory+File.separator+"modules.xml", partition);
        return partition;
    }

    /**
     * Load mapping configuration from a file
     *
     * @param filename the configuration file (ie. mapping.xml)
     * @throws Exception
     */
    public void loadMappingConfig(String filename, Partition partition) throws Exception {
        MappingRule mappingRule = new MappingRule();
        mappingRule.setFile(filename);
        loadMappingConfig(null, null, mappingRule, partition);
    }

    /**
     * Load mapping configuration from a file
     *
     * @param dir
     * @param baseDn
     * @param mappingRule
     * @throws Exception
     */
    public void loadMappingConfig(File dir, String baseDn, MappingRule mappingRule, Partition partition) throws Exception {
        File file = new File(dir, mappingRule.getFile());
        if (!file.exists()) return;
        //log.debug("Loading mapping rule from: "+file.getAbsolutePath());

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/partition/mapping-digester-rules.xml");
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
                loadMappingConfig(file.getParentFile(), baseDn, mr, partition);

            } else if (object instanceof EntryMapping) {

                EntryMapping ed = (EntryMapping)object;
                if (ed.getDn() == null) {
                    ed.setDn(baseDn);

                } else if (baseDn != null) {
                    String parentDn = ed.getParentDn();
                    ed.setParentDn(parentDn == null ? baseDn : parentDn+","+baseDn);
                }

                convert(ed);

                partition.addEntryMapping(ed);

                Collection childDefinitions = ed.getChildMappings();
                for (Iterator j=childDefinitions.iterator(); j.hasNext(); ) {
                    MappingRule mr = (MappingRule)j.next();
                    loadMappingConfig(file.getParentFile(), ed.getDn(), mr, partition);
                }
            }
        }
    }

    public void convert(EntryMapping ed) throws Exception {
        DefaultInterpreter interpreter = new DefaultInterpreter();

        for (Iterator i=ed.getAttributeMappings().iterator(); i.hasNext(); ) {
            AttributeMapping ad = (AttributeMapping)i.next();

            if (ad.getConstant() != null) continue;
            if (ad.getVariable() != null) continue;

            Expression expression = ad.getExpression();
            if (expression.getForeach() != null) continue;
            if (expression.getVar() != null) continue;

            String script = expression.getScript();
            Collection tokens = interpreter.parse(script);

            if (tokens.size() == 1) {

                Token token = (Token)tokens.iterator().next();
                if (token.getType() != Token.STRING_LITERAL) continue;

                String constant = script.substring(1, script.length()-1);
                ad.setConstant(constant);
                ad.setExpression(null);

                //log.debug("Converting "+script+" into constant.");

            } else if (tokens.size() == 3) {

                Iterator iterator = tokens.iterator();
                Token sourceName = (Token)iterator.next();
                if (sourceName.getType() != Token.IDENTIFIER) continue;

                Token dot = (Token)iterator.next();
                if (dot.getType() != Token.DOT) continue;

                Token fieldName = (Token)iterator.next();
                if (fieldName.getType() != Token.IDENTIFIER) continue;

                ad.setVariable(sourceName.getImage()+"."+fieldName.getImage());
                ad.setExpression(null);

                //log.debug("Converting "+script+" into variable.");
            }
        }

        for (Iterator i=ed.getSourceMappings().iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            for (Iterator j=sourceMapping.getFieldMappings().iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();

                if (fieldMapping.getConstant() != null) continue;
                if (fieldMapping.getVariable() != null) continue;

                Expression expression = fieldMapping.getExpression();
                if (expression.getForeach() != null) continue;
                if (expression.getVar() != null) continue;

                String script = expression.getScript();
                Collection tokens = interpreter.parse(script);

                if (tokens.size() != 1) continue;
                Token token = (Token)tokens.iterator().next();

                if (token.getType() == Token.STRING_LITERAL) {
                    String constant = token.getImage();
                    constant = constant.substring(1, constant.length()-1);
                    fieldMapping.setConstant(constant);
                    fieldMapping.setExpression(null);

                    //log.debug("Converting "+script+" into constant.");

                } else if (token.getType() == Token.IDENTIFIER) {
                    String variable = token.getImage();
                    fieldMapping.setVariable(variable);
                    fieldMapping.setExpression(null);

                    //log.debug("Converting "+script+" into variable.");
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
    public void loadModulesConfig(String filename, Partition partition) throws Exception {
        File file = new File(filename);
        if (!file.exists()) return;
        loadModulesConfig(file, partition);
    }

    /**
     * Load mapping configuration from a file
     *
     * @param file the configuration file (ie. modules.xml)
     * @throws Exception
     */
	public void loadModulesConfig(File file, Partition partition) throws Exception {
        //log.debug("Loading modules configuration from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/partition/modules-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
		digester.setValidating(false);
        digester.setClassLoader(cl);
		digester.push(partition);
		digester.parse(file);
	}

    /**
     * Load connections configuration from a file
     *
     * @param filename the configuration file (ie. connections.xml)
     * @throws Exception
     */
    public void loadConnectionsConfig(String filename, Partition partition) throws Exception {
        File file = new File(filename);
        if (!file.exists()) return;
        loadConnectionsConfig(file, partition);
    }

	/**
	 * Load sources configuration from a file
	 *
	 * @param file the configuration file (ie. sources.xml)
	 * @throws Exception
	 */
	public void loadConnectionsConfig(File file, Partition partition) throws Exception {
		//log.debug("Loading source configuration from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/partition/connections-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setValidating(false);
        digester.setClassLoader(cl);
        digester.push(partition);
        digester.parse(file);
	}

    /**
     * Load sources configuration from a file
     *
     * @param filename the configuration file (ie. sources.xml)
     * @throws Exception
     */
    public void loadSourcesConfig(String filename, Partition partition) throws Exception {
        File file = new File(filename);
        if (!file.exists()) return;
        loadSourcesConfig(file, partition);
    }

	/**
	 * Load sources configuration from a file
	 *
	 * @param file the configuration file (ie. sources.xml)
	 * @throws Exception
	 */
	public void loadSourcesConfig(File file, Partition partition) throws Exception {
		//log.debug("Loading source configuration from: "+file.getAbsolutePath());
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/partition/sources-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setValidating(false);
        digester.setClassLoader(cl);
        digester.push(partition);
        digester.parse(file);
	}
}
