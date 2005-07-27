/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;


import java.util.*;

import org.apache.log4j.Logger;
import org.safehaus.penrose.config.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.*;

/**
 * @author Endi S. Dewata
 */
public class TransformEngine {

    public Logger log = Logger.getLogger(Penrose.TRANSFORM_LOGGER);

    public Penrose penrose;
    public Config config;

    public int joinDebug = 0;
    public int crossProductDebug = 0;

    public TransformEngine(Penrose penrose) {
        this.penrose = penrose;
        this.config = penrose.getConfig();
    }

    /**
     * Convert rows into attribute values.
     *
     * Input: Row(value1=a, value2=1)
     * Output: AttributeValues(value1=Collection(a, b, c), value2=Collection(1, 2, 3))
     *
     * @param row
     * @return attribute values
     */
    public AttributeValues convert(Row row) {

        AttributeValues attributes = new AttributeValues();

        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            Set set = new HashSet();
            set.add(value);

            attributes.set(name, set);
        }

        return attributes;
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

    /**
     * Translate attribute values into rows in the source
     *
     * @param source
     * @param row
     * @param pk
     * @param values
     * @return true if this generates a valid primary key
     * @throws Exception
     */
    public boolean translate(Source source, Row row, Row pk, Row values) throws Exception {

    	Interpreter interpreter = penrose.newInterpreter();
        interpreter.set(row);

        Collection fields = source.getFields();

        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            Field field = (Field)j.next();

            String name = field.getName();

            String expression = field.getExpression();
            if (expression == null) {
                if (field.isPrimaryKey()) return false;
                continue;
            }

            Object v = interpreter.eval(expression);
            String value = v == null ? null : v.toString();

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

            if (field.isPrimaryKey()) {
                if (value == null) return false;
                pk.set(name, value);
            }

            values.set(name, value);
        }

        return true;
    }

    /**
     * Translate source row into actual entry attribute values
     * 
     * @param entry the entry
     * @param row the join result row
     * @param pk the primary key(s)
     * @param values attribute values to be populated
     * @return whether we got a valid PK
     * @throws Exception
     */
    public boolean translate(EntryDefinition entry, Row row, Map pk, Row values) throws Exception {

        Interpreter interpreter = penrose.newInterpreter();
        interpreter.set(row);

        Map attributes = entry.getAttributes();
        
        for (Iterator j=attributes.values().iterator(); j.hasNext(); ) {
            AttributeDefinition attribute = (AttributeDefinition)j.next();

            String name = attribute.getName();
            Object value = interpreter.eval(attribute.getExpression());

            if (value == null) continue;

            if (attribute.isRdn()) {
                if (value == null) return false;
                pk.put(name, value);
            }
            
            values.set(name, value);
        }

        return true;
    }

    /**
     * Transform source's rows into entry's attribute values.
     *
     * @param entry
     * @param rows
     * @return map of attribute values
     * @throws Exception
     */
    public Map transform(EntryDefinition entry, Collection rows) throws Exception {

        Map map = new HashMap();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            Map pk = new HashMap();
            Row translatedRow = new Row();

            log.debug(" - before: "+row);

            boolean validPK = translate(entry, row, pk, translatedRow);

            log.debug(" - after: "+translatedRow);

            if (!validPK) continue;

            Map values = (Map)map.get(pk);
            if (values == null) {
                values = new HashMap();
                map.put(pk, values);
            }

            for (Iterator k=translatedRow.getNames().iterator(); k.hasNext(); ) {
                String name = (String)k.next();
                String value = (String)translatedRow.get(name);

                Set set = (Set)values.get(name);
                if (set == null) {
                    set = new HashSet();
                    values.put(name, set);
                }
                if (value != null) {
                	set.add(value);
                }

            }
        }

        return map;
    }

    /**
     * Transform entry's attribute values into the source's rows.
     *
     * @param source
     * @param entry
     * @return rows
     * @throws Exception
     */
    public Map transform(Source source, AttributeValues entry) throws Exception {
        Collection rows = convert(entry);
        log.debug("Original rows: "+rows);

        Map map = new HashMap();

        for (Iterator i=rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();
            Row pk = new Row();
            Row translatedRow = new Row();

            boolean validPK = translate(source, row, pk, translatedRow);
            if (!validPK) continue;

            Collection set = (Collection)map.get(pk);
            if (set == null) {
                set = new HashSet();
                map.put(pk, set);
            }
            set.add(translatedRow);
        }

        log.debug("Valid rows: "+map.values());

        Map map2 = new HashMap();

        for (Iterator i=map.keySet().iterator(); i.hasNext(); ) {
            Row pk = (Row)i.next();
            Set setOfRows = (Set)map.get(pk);

            AttributeValues values = new AttributeValues();

            for (Iterator j=setOfRows.iterator(); j.hasNext(); ) {
                Row translatedRow = (Row)j.next();

                for (Iterator k=translatedRow.getNames().iterator(); k.hasNext(); ) {
                    String name = (String)k.next();
                    String value = (String)translatedRow.get(name);

                    Collection set = (Collection)values.get(name);
                    if (set == null) {
                        set = new HashSet();
                        values.set(name, set);
                    }
                    if (value != null) set.add(value);
                }
            }

            map2.put(pk, values);
        }

        log.debug("Translated rows: "+map2);

        return map2;
    }

    /**
     * Merge rows into attribute values.
     *
     * @param entry
     * @param rows
     * @return attribute values
     */
    public Map merge(EntryDefinition entry, Collection rows) {
        Map attributes = entry.getAttributes();
        Map entries = new LinkedHashMap(); // use LinkedHashMap to maintain row order

        for (Iterator i = rows.iterator(); i.hasNext(); ) {
            Row row = (Row)i.next();

            // generate primary key from attribute values
            Map pk = new HashMap();
            boolean validPk = true;
            for (Iterator j=attributes.values().iterator(); j.hasNext(); ) {
                AttributeDefinition attribute = (AttributeDefinition)j.next();
                String name = attribute.getName();
                String value = (String)row.get(name);

                if (attribute.isRdn()) {
                    if (value == null) validPk = false;
                    pk.put(name, value);
                }
            }

            if (!validPk) continue;

            log.debug("Merging entry "+pk);

            AttributeValues values = (AttributeValues)entries.get(pk);
            if (values == null) {
                values = new AttributeValues();
                entries.put(pk, values);
            }

            // merging each attribute value
            for (Iterator j = row.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                String value = (String)row.get(name);

                Set set = (Set)values.get(name);
                if (set == null) {
                    set = new TreeSet();
                    values.set(name, set);
                }
                if (value != null) set.add(value);
            }
        }

        return entries;
    }
}
