package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.interpreter.Interpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class EngineTool {

    public static Logger log = LoggerFactory.getLogger(EngineTool.class);

    public static void propagateUp(
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues
    ) throws Exception {

        List<EntryMapping> mappings = new ArrayList<EntryMapping>();

        while (entryMapping != null) {
            mappings.add(entryMapping);
            entryMapping = partition.getMappings().getParent(entryMapping);
        }

        propagate(mappings, sourceValues);
    }

    public static void propagateDown(
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues
    ) throws Exception {

        List<EntryMapping> mappings = new ArrayList<EntryMapping>();

        while (entryMapping != null) {
            mappings.add(0, entryMapping);
            entryMapping = partition.getMappings().getParent(entryMapping);
        }

        propagate(mappings, sourceValues);
    }

    public static void propagate(Collection<EntryMapping> mappings, SourceValues sourceValues) throws Exception {

        boolean debug = log.isDebugEnabled();

        for (EntryMapping entryMapping : mappings) {

            Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();
            for (SourceMapping sourceMapping : sourceMappings) {

                if (debug) log.debug("Propagating source " + sourceMapping.getName());

                Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
                for (FieldMapping fieldMapping : fieldMappings) {

                    String lsourceName = sourceMapping.getName();
                    String lfieldName = fieldMapping.getName();
                    String lhs = lsourceName + "." + lfieldName;

                    String variable = fieldMapping.getVariable();
                    if (variable == null) {
                        if (debug) log.debug("Skipping field " + lhs);
                        continue;
                    }

                    int p = variable.indexOf(".");
                    if (p < 0) {
                        if (debug) log.debug("Skipping field " + lhs);
                        continue;
                    }

                    String rsourceName = variable.substring(0, p);
                    String rfieldName = variable.substring(p+1);

                    Attributes lattributes = sourceValues.get(lsourceName);
                    Attributes rattributes = sourceValues.get(rsourceName);

                    Attribute attribute = rattributes.get(rfieldName);

                    if (attribute == null) {
                        if (debug) log.debug("Skipping field " + lhs);
                        continue;
                    }

                    lattributes.setValues(lfieldName, attribute.getValues());
                    if (debug) log.debug("Propagating field " + lhs + ": " + attribute.getValues());
                }
            }
        }
    }

    public static void propagateDown(
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        List<EntryMapping> mappings = new ArrayList<EntryMapping>();

        while (entryMapping != null) {
            mappings.add(0, entryMapping);
            entryMapping = partition.getMappings().getParent(entryMapping);
        }

        propagate(mappings, sourceValues, interpreter);
    }

    public static void propagate(
            Collection<EntryMapping> mappings,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        for (EntryMapping entryMapping : mappings) {

            Collection<SourceMapping> sourceMappings = entryMapping.getSourceMappings();
            for (SourceMapping sourceMapping : sourceMappings) {

                if (debug) log.debug("Propagating source " + sourceMapping.getName());

                Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
                for (FieldMapping fieldMapping : fieldMappings) {

                    String variable = fieldMapping.getVariable();

                    if (variable != null) {
                        propagateVariable(
                                sourceValues,
                                sourceMapping,
                                fieldMapping,
                                variable
                        );

                    } else {
                        String lsourceName = sourceMapping.getName();
                        String lfieldName = fieldMapping.getName();
                        String lhs = lsourceName + "." + lfieldName;

                        Object value = interpreter.eval(fieldMapping);
                        if (value == null) {
                            if (debug) log.debug("Skipping field " + lhs);
                            continue;
                        }

                        if (debug) log.debug("Propagating field " + lhs + ": " + value);

                        Attributes lattributes = sourceValues.get(lsourceName);
                        
                        if (value instanceof Collection) {
                            lattributes.addValues(lfieldName, (Collection)value);
                        } else {
                            lattributes.addValue(lfieldName, value);
                        }
                    }
                }
            }
        }
    }

    public static void propagateVariable(
            SourceValues sourceValues,
            SourceMapping sourceMapping,
            FieldMapping fieldMapping,
            String variable
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String lsourceName = sourceMapping.getName();
        String lfieldName = fieldMapping.getName();
        String lhs = lsourceName + "." + lfieldName;

        int p = variable.indexOf(".");
        if (p < 0) {
            if (debug) log.debug("Skipping field " + lhs);
            return;
        }

        String rsourceName = variable.substring(0, p);
        String rfieldName = variable.substring(p+1);

        Attributes lattributes = sourceValues.get(lsourceName);
        Attributes rattributes = sourceValues.get(rsourceName);

        Attribute lattribute = lattributes.get(lfieldName);

        if (lattribute != null && !lattribute.isEmpty()) {
            if (debug) log.debug("Skipping field " + lhs);
            return;
        }

        Attribute rattribute = rattributes.get(rfieldName);

        if (rattribute == null) {
            if (debug) log.debug("Skipping field " + lhs);
            return;
        }

        lattributes.setValues(lfieldName, rattribute.getValues());
        if (debug) log.debug("Propagating field " + lhs + ": " + rattribute.getValues());
    }

}
