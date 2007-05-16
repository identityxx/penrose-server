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
package org.safehaus.penrose.engine;


import java.util.*;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.DN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class TransformEngine {

    static Logger log = LoggerFactory.getLogger(TransformEngine.class);

    public Engine engine;

    public int joinDebug = 0;
    public static int crossProductDebug = 0;

    public TransformEngine(Engine engine) {
        this.engine = engine;
    }

    /**
     * Convert attribute values into rows.
     *
     * Input: Attributes(attr1=[a, b, c], attr2=[1, 2, 3])
     * Output: List(RDN(attr1=a, attr2=1), RDN(attr1=a, attr2=2), ... )
     *
     * @param attributes
     * @return collection of Rows
     */
    public static Collection<RDN> convert(Attributes attributes) {
        Map<String,Collection> map = new HashMap<String,Collection>();
        for (Attribute attribute : attributes.getAll()) {
            map.put(attribute.getName(), attribute.getValues());
        }
        return convert(map);
    }
    
    /**
     * Convert map of values into rows.
     *
     * Input: Map(value1=Collection(a, b, c), value2=Collection(1, 2, 3))
     * Output: List(RDN(value1=a, value2=1), RDN(value1=a, value2=2), ... )
     *
     * @param values Map of collections.
     * @return collection of Rows
     */
    public static Collection<RDN> convert(Map<String,Collection> values) {
        List<String> names = new ArrayList<String>(values.keySet());
        List<RDN> results = new ArrayList<RDN>();
        Map<String,Object> temp = new HashMap<String,Object>();

        if (crossProductDebug >= 65535) {
            log.debug("Generating cross product:");
            log.debug("Names: "+names);
        }

        convert(values, names, 0, temp, results);

        return results;
    }

    public static void convert(
            Map<String,Collection> values,
            List names,
            int pos,
            Map<String,Object> temp,
            Collection<RDN> results
    ) {

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

            RDN rdn = new RDN(temp);
            results.add(rdn);

            //if (crossProductDebug >= 65535) {
                //log.debug("Generated: "+rdn);
            //}

        } else {
            if (crossProductDebug >= 65535) {
                //log.debug("Temp is empty: "+temp);
            }
        }
    }

    public void translate(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            DN dn,
            SourceValues input,
            SourceValues output
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        SourceConfig sourceConfig = partition.getSources().getSourceConfig(sourceMapping.getSourceName());
/*
        Collection relationships = entryMapping.getRelationships();
        if (relationships.size() > 0) {
            Relationship relationship = (Relationship)relationships.iterator().next();

            String lhs = relationship.getLhs();
            int lindex = lhs.indexOf(".");
            String lsource = lhs.substring(0, lindex);
            String lfield = lhs.substring(lindex+1);

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            String rsource = rhs.substring(0, rindex);
            String rfield = rhs.substring(rindex+1);

            String parentSource = lsource;
            String parentField = lfield;
            String fieldName = rfield;
            String sourceName = rsource;

            if (entryMapping.getSourceMapping(lsource) != null) {
                parentSource = rsource;
                parentField = rfield;
                fieldName = lfield;
                sourceName = lsource;
            }

            EntryMapping parentMapping = partition.getParent(entryMapping);
            if (parentMapping != null && parentMapping.getSourceMapping(parentSource) != null) {
                DN parentDn = dn.getParentDn();
                RDN parentRdn = parentDn.getRdn();

                SourceMapping parentSourceMapping = parentMapping.getSourceMapping(parentSource);

                Object value = null;

                Collection fieldMappings = parentSourceMapping.getFieldMappings(parentField);
                for (Iterator i=fieldMappings.iterator(); i.hasNext(); ) {
                    FieldMapping fieldMapping = (FieldMapping)i.next();
                    if (fieldMapping.getVariable() == null) continue;

                    String variable = fieldMapping.getVariable();
                    value = parentRdn.get(variable);
                    break;
                }

                if (debug) log.debug("Translating "+parentSource+"."+parentField+" => "+sourceName+"."+fieldName+": "+value);

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

                if (fieldConfig.isPrimaryKey()) {
                    output.set("primaryKey."+fieldName, value);
                }

                output.set(fieldName, value);
            }
        }
*/
        Interpreter interpreter = engine.getInterpreterManager().newInstance();
        interpreter.set(input);

        Collection fields = sourceMapping.getFieldMappings();
        for (Iterator i =fields.iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();
            String fieldName = fieldMapping.getName();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            Object newValues = interpreter.eval(fieldMapping);
            if (newValues == null) {
                if (debug) log.debug("Field "+fieldName+" is empty.");
                continue;
            }

            if (fieldConfig.isPrimaryKey()) {
                output.set("primaryKey."+fieldName, newValues);
            }

            output.add(fieldName, newValues);
        }

        interpreter.clear();
    }
}
