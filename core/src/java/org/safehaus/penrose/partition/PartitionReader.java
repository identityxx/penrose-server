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

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.interpreter.Token;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PartitionReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    private String home;

    URL partitionDtdUrl;
    URL connectionsDtdUrl;
    URL sourcesDtdUrl;
    URL mappingDtdUrl;
    URL modulesDtdUrl;

    public PartitionReader() {
        this(null);
    }

    public PartitionReader(String home) {
        this.home = home;

        ClassLoader cl = getClass().getClassLoader();

        partitionDtdUrl = cl.getResource("org/safehaus/penrose/partition/partition.dtd");
        connectionsDtdUrl = cl.getResource("org/safehaus/penrose/connection/connections.dtd");
        sourcesDtdUrl = cl.getResource("org/safehaus/penrose/source/sources.dtd");
        mappingDtdUrl = cl.getResource("org/safehaus/penrose/mapping/mapping.dtd");
        modulesDtdUrl = cl.getResource("org/safehaus/penrose/module/modules.dtd");
    }

    public PartitionConfig readPartitionConfig(String path) throws Exception {

        PartitionConfig partitionConfig = new PartitionConfig();

        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+File.separator+path;
        }

        String filename = (path == null ? "" : path+File.separator)+"partition.xml";
        log.debug("Loading "+filename);

        File pathFile = new File(path);
        File file = new File(filename);
        if (!file.exists()) {
            log.debug("File "+filename+" not found");

            partitionConfig.setName(pathFile.getName());
            partitionConfig.setPath(path);
            return partitionConfig;
        }

        //log.debug("Loading partition configuration from: "+file.getAbsolutePath());

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/partition/partition-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
		digester.push(partitionConfig);
		digester.parse(file);

        return partitionConfig;
    }

    public Partition read(PartitionConfig partitionConfig) throws Exception {
        return read(partitionConfig, partitionConfig.getPath());
    }

    public Partition read(PartitionConfig partitionConfig, String path) throws Exception {
        Partition partition = new Partition(partitionConfig);
        loadConnectionsConfig(path, partition);
        loadSourcesConfig(path, partition);
        loadMappingConfig(path, partition);
        loadModulesConfig(path, partition);
        return partition;
    }

    /**
     * Load mapping configuration from a file
     *
     * @throws Exception
     */
    public void loadMappingConfig(String path, Partition partition) throws Exception {
        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+File.separator+path;
        }
        String filename = (path == null ? "" : path+File.separator)+"mapping.xml";
        log.debug("Loading "+filename);

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

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/mapping/mapping-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
		digester.push(partition);
		digester.parse(file);
/*
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
*/
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
     * @throws Exception
     */
    public void loadModulesConfig(String path, Partition partition) throws Exception {
        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+File.separator+path;
        }
        String filename = (path == null ? "" : path+File.separator)+"modules.xml";
        log.debug("Loading "+filename);

        File file = new File(filename);
        if (!file.exists()) return;

        //log.debug("Loading modules configuration from: "+file.getAbsolutePath());

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/module/modules-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
		digester.push(partition);
		digester.parse(file);
	}

    /**
     * Load connections configuration
     *
     * @throws Exception
     */
    public void loadConnectionsConfig(String path, Partition partition) throws Exception {
        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+File.separator+path;
        }
        String filename = (path == null ? "" : path+File.separator)+"connections.xml";
        log.debug("Loading "+filename);

        File file = new File(filename);
        if (!file.exists()) return;

		//log.debug("Loading source configuration from: "+file.getAbsolutePath());

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/connection/connections-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
        digester.push(partition);
        digester.parse(file);
	}

    /**
     * Load sources configuration from a file
     *
     * @throws Exception
     */
    public void loadSourcesConfig(String path, Partition partition) throws Exception {
        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+File.separator+path;
        }
        String filename = (path == null ? "" : path+File.separator)+"sources.xml";
        log.debug("Loading "+filename);

        File file = new File(filename);
        if (!file.exists()) return;

		//log.debug("Loading source configuration from: "+file.getAbsolutePath());

        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/source/sources-digester-rules.xml");
		Digester digester = DigesterLoader.createDigester(url);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
        digester.push(partition);
        digester.parse(file);
	}

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        //log.debug("Resolving "+publicId+" "+systemId);

        int i = systemId.lastIndexOf("/");
        String file = systemId.substring(i+1);
        //log.debug("=> "+file);

        URL url = null;

        if ("partition.dtd".equals(file)) {
            url = partitionDtdUrl;

        } else if ("connections.dtd".equals(file)) {
            url = connectionsDtdUrl;

        } else if ("sources.dtd".equals(file)) {
            url = sourcesDtdUrl;

        } else if ("mapping.dtd".equals(file)) {
            url = mappingDtdUrl;

        } else if ("modules.dtd".equals(file)) {
            url = modulesDtdUrl;
        }

        if (url == null) return null;

        return new InputSource(url.openStream());
    }
}
