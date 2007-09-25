package org.safehaus.penrose.mapping;

import org.safehaus.penrose.interpreter.DefaultInterpreter;
import org.safehaus.penrose.interpreter.Token;
import org.safehaus.penrose.directory.DirectoryConfig;
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.directory.FieldMapping;
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

    public Logger log = LoggerFactory.getLogger(getClass());

    URL dtdUrl;
    URL digesterUrl;

    Digester digester;

    public MappingReader() {

        ClassLoader cl = getClass().getClassLoader();

        dtdUrl = cl.getResource("org/safehaus/penrose/mapping/mapping.dtd");
        digesterUrl = cl.getResource("org/safehaus/penrose/mapping/mapping-digester-rules.xml");

        digester = DigesterLoader.createDigester(digesterUrl);
        digester.setEntityResolver(this);
        digester.setValidating(true);
        digester.setClassLoader(cl);
    }

    public void read(File file, DirectoryConfig mappings) throws Exception {
		digester.push(mappings);
		digester.parse(file);
        digester.pop();
    }

    public void convert(EntryMapping entryMapping) throws Exception {
        DefaultInterpreter interpreter = new DefaultInterpreter();

        for (AttributeMapping attributeMapping : entryMapping.getAttributeMappings()) {

            if (attributeMapping.getConstant() != null) continue;
            if (attributeMapping.getVariable() != null) continue;

            Expression expression = attributeMapping.getExpression();
            if (expression.getForeach() != null) continue;
            if (expression.getVar() != null) continue;

            String script = expression.getScript();
            Collection tokens = interpreter.parse(script);

            if (tokens.size() == 1) {

                Token token = (Token) tokens.iterator().next();
                if (token.getType() != Token.STRING_LITERAL) continue;

                String constant = script.substring(1, script.length() - 1);
                attributeMapping.setConstant(constant);
                attributeMapping.setExpression(null);

                //log.debug("Converting "+script+" into constant.");

            } else if (tokens.size() == 3) {

                Iterator iterator = tokens.iterator();
                Token sourceName = (Token) iterator.next();
                if (sourceName.getType() != Token.IDENTIFIER) continue;

                Token dot = (Token) iterator.next();
                if (dot.getType() != Token.DOT) continue;

                Token fieldName = (Token) iterator.next();
                if (fieldName.getType() != Token.IDENTIFIER) continue;

                attributeMapping.setVariable(sourceName.getImage() + "." + fieldName.getImage());
                attributeMapping.setExpression(null);

                //log.debug("Converting "+script+" into variable.");
            }
        }

        for (SourceMapping sourceMapping : entryMapping.getSourceMappings()) {

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
        return new InputSource(dtdUrl.openStream());
    }
}
