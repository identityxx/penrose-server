package org.safehaus.penrose.mapping;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.interpreter.Token;
import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class MappingReader implements EntityResolver {

    Logger log = LoggerFactory.getLogger(getClass());

    String home;

    public MappingReader(String home) {
        this.home = home;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public void read(String path, Partition partition) throws Exception {
        if (path == null) {
            path = home;
        } else if (home != null) {
            path = home+ File.separator+path;
        }
        String filename = (path == null ? "" : path+File.separator)+"mapping.xml";
        log.debug("Loading "+filename);

        MappingRule mappingRule = new MappingRule();
        mappingRule.setFile(filename);
        loadMappingConfig(null, null, mappingRule, partition);
    }

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
                read(file.getParentFile(), baseDn, mr, partition);

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
                    read(file.getParentFile(), ed.getDn(), mr, partition);
                }
            }
        }
*/
    }

    public void convert(EntryMapping ed) throws Exception {
        DefaultInterpreter interpreter = new DefaultInterpreter();

        for (AttributeMapping ad : ed.getAttributeMappings()) {

            if (ad.getConstant() != null) continue;
            if (ad.getVariable() != null) continue;

            Expression expression = ad.getExpression();
            if (expression.getForeach() != null) continue;
            if (expression.getVar() != null) continue;

            String script = expression.getScript();
            Collection tokens = interpreter.parse(script);

            if (tokens.size() == 1) {

                Token token = (Token) tokens.iterator().next();
                if (token.getType() != Token.STRING_LITERAL) continue;

                String constant = script.substring(1, script.length() - 1);
                ad.setConstant(constant);
                ad.setExpression(null);

                //log.debug("Converting "+script+" into constant.");

            } else if (tokens.size() == 3) {

                Iterator iterator = tokens.iterator();
                Token sourceName = (Token) iterator.next();
                if (sourceName.getType() != Token.IDENTIFIER) continue;

                Token dot = (Token) iterator.next();
                if (dot.getType() != Token.DOT) continue;

                Token fieldName = (Token) iterator.next();
                if (fieldName.getType() != Token.IDENTIFIER) continue;

                ad.setVariable(sourceName.getImage() + "." + fieldName.getImage());
                ad.setExpression(null);

                //log.debug("Converting "+script+" into variable.");
            }
        }

        for (SourceMapping sourceMapping : ed.getSourceMappings()) {

            for (FieldMapping fieldMapping : sourceMapping.getFieldMappings()) {

                if (fieldMapping.getConstant() != null) continue;
                if (fieldMapping.getVariable() != null) continue;

                Expression expression = fieldMapping.getExpression();
                if (expression.getForeach() != null) continue;
                if (expression.getVar() != null) continue;

                String script = expression.getScript();
                Collection tokens = interpreter.parse(script);

                if (tokens.size() != 1) continue;
                Token token = (Token) tokens.iterator().next();

                if (token.getType() == Token.STRING_LITERAL) {
                    String constant = token.getImage();
                    constant = constant.substring(1, constant.length() - 1);
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

    public InputSource resolveEntity(String publicId, String systemId) throws IOException {
        ClassLoader cl = getClass().getClassLoader();
        URL url = cl.getResource("org/safehaus/penrose/mapping/mapping.dtd");
        return new InputSource(url.openStream());
    }
}
