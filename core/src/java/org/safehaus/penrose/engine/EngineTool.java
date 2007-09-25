package org.safehaus.penrose.engine;

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.FieldMapping;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.interpreter.InterpreterManager;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class EngineTool {

    public static Logger log = LoggerFactory.getLogger(EngineTool.class);
    public static boolean debug = log.isDebugEnabled();

    public static void propagateUp(
            Entry entry,
            SourceValues sourceValues
    ) throws Exception {

        List<Entry> entries = new ArrayList<Entry>();

        while (entry != null) {
            entries.add(entry);
            entry = entry.getParent();
        }

        propagate(entries, sourceValues);
    }

    public static void propagateDown(
            Entry entry,
            SourceValues sourceValues
    ) throws Exception {

        List<Entry> entries = new ArrayList<Entry>();

        while (entry != null) {
            entries.add(0, entry);
            entry = entry.getParent();
        }

        propagate(entries, sourceValues);
    }

    public static void propagate(Collection<Entry> entries, SourceValues sourceValues) throws Exception {

        for (Entry entry : entries) {

            Collection<SourceMapping> sourceMappings = entry.getSourceMappings();
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
            Entry entry,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        List<Entry> path = new ArrayList<Entry>();

        while (entry != null) {
            path.add(0, entry);
            entry = entry.getParent();
        }

        propagate(path, sourceValues, interpreter);
    }

    public static void propagate(
            Collection<Entry> path,
            SourceValues sourceValues,
            Interpreter interpreter
    ) throws Exception {

        for (Entry entry : path) {

            Collection<SourceMapping> sourceMappings = entry.getSourceMappings();
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

        String lsourceName = sourceMapping.getName();
        String lfieldName = fieldMapping.getName();

        int p = variable.indexOf(".");
        if (p < 0) {
            if (debug) log.debug("Skipping field " + lsourceName + "." + lfieldName);
            return;
        }

        String rsourceName = variable.substring(0, p);
        String rfieldName = variable.substring(p+1);

        Attributes lattributes = sourceValues.get(lsourceName);
        Attributes rattributes = sourceValues.get(rsourceName);

        Attribute lattribute = lattributes.get(lfieldName);

        if (lattribute != null && !lattribute.isEmpty()) {
            if (debug) log.debug("Skipping field " + lsourceName + "." + lfieldName);
            return;
        }

        Attribute rattribute = rattributes.get(rfieldName);

        if (rattribute == null) {
            if (debug) log.debug("Skipping field " + lsourceName + "." + lfieldName);
            return;
        }

        lattributes.setValues(lfieldName, rattribute.getValues());
        if (debug) log.debug("Propagating field " + lsourceName + "." + lfieldName + ": " + rattribute.getValues());
    }

    public static void extractSourceValues(
            Entry entry,
            DN dn,
            SourceValues sourceValues
    ) throws Exception {

        Directory directory = entry.getDirectory();
        Partition partition = directory.getPartition();
        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        InterpreterManager interpreterManager = penroseContext.getInterpreterManager();
        Interpreter interpreter = interpreterManager.newInstance();

        if (debug) log.debug("Extracting source values from "+dn);

        extractSourceValues(
                interpreter,
                dn,
                entry,
                sourceValues
        );
    }

    public static void extractSourceValues(
            Interpreter interpreter,
            DN dn,
            Entry entry,
            SourceValues sourceValues
    ) throws Exception {

        DN parentDn = dn.getParentDn();
        Entry parent = entry.getParent();

        if (parentDn != null && parent != null) {
            extractSourceValues(interpreter, parentDn, parent, sourceValues);
        }

        RDN rdn = dn.getRdn();
        Collection<SourceMapping> sourceMappings = entry.getSourceMappings();

        //if (sourceMappings.isEmpty()) return;
        //SourceMapping sourceMapping = sourceMappings.iterator().next();

        //interpreter.set(sourceValues);
        interpreter.set(rdn);

        for (SourceMapping sourceMapping : sourceMappings) {
            extractSourceValues(
                    interpreter,
                    rdn,
                    entry,
                    sourceMapping,
                    sourceValues
            );
        }

        interpreter.clear();
    }

    public static void extractSourceValues(
            Interpreter interpreter,
            RDN rdn,
            Entry entry,
            SourceMapping sourceMapping,
            SourceValues sourceValues
    ) throws Exception {

        if (debug) log.debug("Extracting source "+sourceMapping.getName()+" from RDN: "+rdn);

        Directory directory = entry.getDirectory();
        Partition partition = directory.getPartition();

        Attributes attributes = sourceValues.get(sourceMapping.getName());

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SourceConfigs sources = partitionConfig.getSourceConfigs();
        SourceConfig sourceConfig = sources.getSourceConfig(sourceMapping.getSourceName());

        Collection<FieldMapping> fieldMappings = sourceMapping.getFieldMappings();
        for (FieldMapping fieldMapping : fieldMappings) {
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldMapping.getName());

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if ("INTEGER".equals(fieldConfig.getType()) && value instanceof String) {
                value = Integer.parseInt((String)value);
            }

            attributes.addValue(fieldMapping.getName(), value);

            String fieldName = sourceMapping.getName() + "." + fieldMapping.getName();
            if (debug) log.debug(" => " + fieldName + ": " + value);
        }
    }
}
