/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;


import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.mapping.*;

/**
 * @author Endi S. Dewata
 */
public class TransformEngine {

    Logger log = LoggerFactory.getLogger(getClass());

    public Penrose penrose;

    public int joinDebug = 0;
    public int crossProductDebug = 0;

    public TransformEngine(Penrose penrose) {
        this.penrose = penrose;
    }

    /**
     * Convert attribute values into rows.
     *
     * Input: AttributeValues(value1=Collection(a, b, c), value2=Collection(1, 2, 3))
     * Output: List(Row(value1=a, value2=1), Row(value1=a, value2=2), ... )
     *
     * @param attributes
     * @return collection of Rows
     */
    public Collection convert(AttributeValues attributes) {
        return convert(attributes.getValues());
    }

    /**
     * Convert map of values into rows.
     *
     * Input: Map(value1=Collection(a, b, c), value2=Collection(1, 2, 3))
     * Output: List(Row(value1=a, value2=1), Row(value1=a, value2=2), ... )
     *
     * @param values Map of collections.
     * @return collection of Rows
     */
    public Collection convert(Map values) {
        List names = new ArrayList(values.keySet());
        List results = new ArrayList();
        Map temp = new HashMap();

        if (crossProductDebug >= 65535) {
            log.debug("Generating cross product:");
            log.debug("Names: "+names);
        }

        convert(values, names, 0, temp, results);

        return results;
    }

    public void convert(Map values, List names, int pos, Map temp, Collection results) {

        if (pos < names.size()) {

            // get each attribute's values
            String name = (String)names.get(pos);
            Collection c = (Collection)values.get(name);

            if (c.isEmpty()) {
                c = new HashSet();
                c.add(null);
            }

            if (crossProductDebug >= 65535) {
            	//log.debug(name+": "+c);
            }

            for (Iterator iterator = c.iterator(); iterator.hasNext(); ) {
                Object value = iterator.next();

                temp.put(name, value);

                convert(values, names, pos+1, temp, results);
            }

        } else if (!temp.isEmpty()) {

            Row map = new Row(temp);
            results.add(map);

            //if (crossProductDebug >= 65535) {
            	//log.debug("Generated: "+map);
            //}

        } else {
            if (crossProductDebug >= 65535) {
            	//log.debug("Temp is empty: "+temp);
            }
        }
    }

    public Row translate(EntryDefinition entry, AttributeValues input, AttributeValues output) throws Exception {

        Interpreter interpreter = penrose.newInterpreter();
        interpreter.set(input);

        Row pk = new Row();
        Collection attributes = entry.getAttributeDefinitions();

        for (Iterator j=attributes.iterator(); j.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)j.next();

            String name = attribute.getName();
            Expression expression = attribute.getExpression();
            String foreach = expression.getForeach();
            String var = expression.getVar();

            Object value = null;
            if (foreach == null) {
                //log.debug("Evaluating expression: "+expression);
                value = interpreter.eval(expression.getScript());

            } else {
                //log.debug("Evaluating expression: "+expression);

                Collection values = input.get(foreach);
                //log.debug("Values: "+values);

                if (values != null) {

                    Collection newValues = new ArrayList();
                    for (Iterator i=values.iterator(); i.hasNext(); ) {
                        Object o = i.next();
                        interpreter.set(var, o);
                        value = interpreter.eval(attribute.getExpression().getScript());
                        //log.debug(" - "+value);
                        newValues.add(value);
                    }

                    value = newValues;
                }
            }

            //log.debug("Result: "+value);

            if (value == null) continue;

            if (attribute.isRdn()) {
                if (value == null) return null;
                pk.set(name, value);
            }

            output.add(name, value);
        }

        return pk;
    }

    public Row translate(Source source, AttributeValues input, AttributeValues output) throws Exception {

        Config config = penrose.getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Interpreter interpreter = penrose.newInterpreter();
        interpreter.set(input);

        Row pk = new Row();
        Collection fields = source.getFields();

        log.debug("Translating for source "+source.getName()+":");
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            Field field = (Field)j.next();
            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());

            String name = field.getName();
            //log.debug(" - "+name);

            Expression expression = field.getExpression();

            if (expression == null) {
                if (fieldDefinition.isPrimaryKey()) return null;
                continue;
            }

            String script = expression.getScript();
            String foreach = expression.getForeach();
            String var = expression.getVar();
            //log.debug("   - expression: "+foreach+": "+script);

            Collection newValues = new ArrayList();
            if (foreach == null) {
                Object value = interpreter.eval(expression.getScript());
                if (value != null) newValues.add(value);

            } else {

                Collection oldValues = input.get(foreach);
                //log.debug("Values: "+oldValues);

                if (oldValues != null) {

                    for (Iterator i=oldValues.iterator(); i.hasNext(); ) {
                        Object o = i.next();
                        interpreter.set(var, o);
                        Object value = interpreter.eval(script);
                        if (value == null) continue;

                        newValues.add(value);
                        //log.debug(" - "+value);
                    }
                }
            }

/*
            if (field.getEncryption() != null) {
                // if field encryption is enabled

                String encryptionMethod = PasswordUtil.getEncryptionMethod(value);
                String encodingMethod = PasswordUtil.getEncodingMethod(value);
                String encryptedPassword = PasswordUtil.getEncryptedPassword(value);

                if (encryptionMethod == null) {
                    // if value is not encryption then encrypt value

                    value = PasswordUtil.encrypt(field.getEncryption(), field.getEncoding(), value);
                    log.debug("TRANSLATE - encrypt with "+field.getEncryption()+": "+value);

                } else if (field.getEncryption().equals(encryptionMethod)) {
                    // if field encryption is equal to value encryption

                    value = encryptedPassword;
                    log.debug("TRANSLATE - already encrypted: "+value);

                } else {
                	log.debug("TRANSLATE - unchanged: "+value);
                }
            }
*/
            if (newValues.size() == 0) continue;

            log.debug(" - "+name+": "+newValues);

            if (fieldDefinition.isPrimaryKey()) {
                if (newValues.size() == 0) return null;
                pk.set(name, newValues);
            }

            output.add(name, newValues);
        }

        return pk;
    }

    public Map split(Source source, AttributeValues entry) throws Exception {

        Config config = penrose.getConfig(source);
        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        AttributeValues output = new AttributeValues();
        Row m = translate(source, entry, output);
        log.debug("PKs: "+m);
        log.debug("Output: "+output);

        Collection rows = convert(output);
        Map map = new TreeMap();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            log.debug(" - "+row);

            AttributeValues av = new AttributeValues();
            av.add(row);

            Row pk = new Row();
            Collection fields = source.getFields();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(field.getName());
                if (!fieldDefinition.isPrimaryKey()) continue;
                pk.set(field.getName(), row.get(field.getName()));
            }

            map.put(pk, av);
        }

        return map;
    }

}
