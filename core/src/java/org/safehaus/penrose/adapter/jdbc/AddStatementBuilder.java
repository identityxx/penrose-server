package org.safehaus.penrose.adapter.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.jdbc.InsertStatement;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    JDBCAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    AddRequest request;
    AddResponse response;

    Collection statements = new ArrayList();

    public AddStatementBuilder(
            JDBCAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        this.adapter = adapter;

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sourceMappings = sourceMappings;
        primarySourceMapping = (SourceMapping)sourceMappings.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        PenroseContext penroseContext = adapter.getPenroseContext();
        interpreter = penroseContext.getInterpreterManager().newInstance();
    }

    public Collection generate() throws Exception {

        int sourceCounter = 0;
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); sourceCounter++) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            if (sourceCounter == 0) {
                generatePrimaryStatement(sourceMapping);
            } else {
                generateSecondaryStatements(sourceMapping);
            }
        }

        return statements;
    }

    public void generatePrimaryStatement(
            SourceMapping sourceMapping
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        InsertStatement statement = new InsertStatement();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        statement.setTable(table);

        interpreter.set(sourceValues);

        Attributes attributes = request.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String attributeName = attribute.getName();
            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(attributeName, attributeValue);
        }

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            String fieldName = fieldMapping.getName();
            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addColumn(fieldConfig.getOriginalName());
            statement.addParameter(new Parameter(fieldConfig, value));
        }

        interpreter.clear();

        statements.add(statement);
    }

    public void generateSecondaryStatements(
            SourceMapping sourceMapping
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        Attributes attributes = request.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            for (Iterator j=attributeValues.iterator(); j.hasNext(); ) {
                Object attributeValue = j.next();

                interpreter.set(sourceValues);
                interpreter.set(attributeName, attributeValue);

                Map values = new HashMap();

                Collection fieldMappings = sourceMapping.getFieldMappings();
                for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
                    FieldMapping fieldMapping = (FieldMapping)k.next();

                    String variable = fieldMapping.getVariable();
                    if (variable != null) {
                        if (variable.indexOf(".") >= 0) continue; // skip foreign key
                    }

                    String fieldName = fieldMapping.getName();
                    Object value = interpreter.eval(entryMapping, fieldMapping);
                    if (value == null) continue;

                    values.put(fieldName, value);
                }

                if (!values.isEmpty()) {
                    generateInsertStatement(
                            sourceMapping,
                            values
                    );
                }

                interpreter.clear();
            }

        }
    }

    public void generateInsertStatement(
            SourceMapping sourceMapping,
            Map values
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Inserting values into "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        InsertStatement statement = new InsertStatement();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        statement.setTable(table);

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            Object value = sourceValues.getOne(variable);
            if (value == null) continue;

            String fieldName = fieldMapping.getName();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addColumn(fieldConfig.getOriginalName());
            statement.addParameter(new Parameter(fieldConfig, value));
        }

        for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();
            Object value = values.get(fieldName);
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addColumn(fieldConfig.getOriginalName());
            statement.addParameter(new Parameter(fieldConfig, value));
        }

        statements.add(statement);
    }
}
