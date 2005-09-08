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
        Map attributes = entry.getAttributes();

        for (Iterator j=attributes.values().iterator(); j.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)j.next();

            String name = attribute.getName();
            Expression expression = attribute.getExpression();
            String variable = expression.getForeach();

            Object value = null;
            if (variable == null) {
                //log.debug("Evaluating expression: "+expression);
                value = interpreter.eval(expression.getScript());

            } else {
                //log.debug("Evaluating expression: "+expression);

                Collection values = input.get(variable);
                //log.debug("Values: "+values);

                if (values != null) {

                    Collection newValues = new ArrayList();
                    for (Iterator i=values.iterator(); i.hasNext(); ) {
                        Object o = i.next();
                        interpreter.set(variable, o);
                        value = interpreter.eval(attribute.getExpression().getScript());
                        //log.debug(" - "+value);
                        newValues.add(value);
                    }

                    // restore old values
                    if (values.size() > 1) {
                        interpreter.set(variable, values);
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

        Interpreter interpreter = penrose.newInterpreter();
        interpreter.set(input);

        Row pk = new Row();
        Collection fields = source.getFields();

        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            Field field = (Field)j.next();

            String name = field.getName();
            Expression expression = field.getExpression();

            if (expression == null) {
                if (field.isPrimaryKey()) return null;
                continue;
            }

            String variable = expression.getForeach();

            Collection newValue = new ArrayList();
            if (variable == null) {
                //log.debug("Evaluating expression: "+expression);
                Object value = interpreter.eval(expression.getScript());
                if (value == null) continue;

                newValue.add(value);

            } else {
                //log.debug("Evaluating expression: "+expression);

                Collection oldValues = input.get(variable);
                //log.debug("Values: "+oldValues);

                if (oldValues != null) {

                    for (Iterator i=oldValues.iterator(); i.hasNext(); ) {
                        Object o = i.next();
                        interpreter.set(variable, o);
                        Object value = interpreter.eval(field.getExpression().getScript());
                        if (value == null) continue;

                        newValue.add(value);
                        //log.debug(" - "+value);
                    }

                    // restore old values
                    if (oldValues.size() > 1) {
                        interpreter.set(variable, oldValues);
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
            //log.debug("Result: "+newValue);

            if (newValue == null) continue;

            if (field.isPrimaryKey()) {
                if (newValue == null) return null;
                pk.set(name, newValue);
            }

            output.add(name, newValue);
        }

        return pk;
    }

    public Map split(Source source, AttributeValues entry) throws Exception {

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
            Collection fields = source.getPrimaryKeyFields();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                pk.set(field.getName(), row.get(field.getName()));
            }

            map.put(pk, av);
        }

        return map;
    }

    public Collection merge(Entry parent, EntryDefinition entryDefinition, Map values) throws Exception {

        Collection results = new ArrayList();

        log.debug("Merging:");
        int counter = 1;

        // merge rows into attribute values
        for (Iterator i = values.keySet().iterator(); i.hasNext(); counter++) {
            Row pk = (Row)i.next();
            log.debug(" - "+pk);

            AttributeValues sourceValues = (AttributeValues)values.get(pk);
            AttributeValues attributeValues = new AttributeValues();

            Row rdn = translate(entryDefinition, sourceValues, attributeValues);
            if (rdn == null) continue;

            //log.debug("   => "+rdn+": "+attributeValues);

            Entry entry = new Entry(rdn+","+parent.getDn(), entryDefinition, sourceValues, attributeValues);
            entry.setParent(parent);
            results.add(entry);

            log.debug("Entry #"+counter+":\n"+entry+"\n");
        }

        return results;
    }

}
