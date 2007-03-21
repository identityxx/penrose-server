package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.session.Modification;
import org.safehaus.penrose.session.ModifyRequest;
import org.safehaus.penrose.session.ModifyResponse;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.jdbc.DeleteStatement;
import org.safehaus.penrose.jdbc.UpdateStatement;
import org.safehaus.penrose.jdbc.InsertStatement;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    JDBCAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    ModifyRequest request;
    ModifyResponse response;

    FilterBuilder filterBuilder;

    Collection statements = new ArrayList();

    public ModifyStatementBuilder(
            JDBCAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
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

        filterBuilder = new FilterBuilder(
                partition,
                entryMapping,
                sourceMappings,
                interpreter
        );

        filterBuilder.init(sourceValues);
    }

    public Collection generate() throws Exception {

        Collection statements = new ArrayList();

        int sourceCounter = 0;
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); sourceCounter++) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            if (sourceCounter == 0) {
                generatePrimaryStatement(statements, sourceMapping);
            } else {
                generateSecondaryStatements(statements, sourceMapping);
            }
        }

        return statements;
    }

    public void generatePrimaryStatement(
            Collection statements,
            SourceMapping sourceMapping
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        UpdateStatement statement = new UpdateStatement();
        Collection tables = statement.getTables();
        Collection columns = statement.getColumns();
        Collection parameters = statement.getParameters();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        tables.add(table);

        Collection modifications = request.getModifications();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            if (debug) {
                switch (type) {
                    case Modification.ADD:
                        log.debug("Adding attribute "+attributeName+": "+attributeValues);
                        break;
                    case Modification.REPLACE:
                        log.debug("Replacing attribute "+attributeName+": "+attributeValues);
                        break;
                    case Modification.DELETE:
                        log.debug("Deleting attribute "+attributeName+": "+attributeValues);
                        break;
                }
            }

            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(sourceValues);
            interpreter.set(attributeName, attributeValue);

            Collection fieldMappings = sourceMapping.getFieldMappings();

            switch (type) {
                case Modification.ADD:
                case Modification.REPLACE:
                    for (Iterator j=fieldMappings.iterator(); j.hasNext(); ) {
                        FieldMapping fieldMapping = (FieldMapping)j.next();

                        Object value = interpreter.eval(entryMapping, fieldMapping);
                        if (value == null) continue;

                        String fieldName = fieldMapping.getName();
                        if (debug) log.debug("Setting field "+fieldName+" to "+value);

                        FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                        columns.add(fieldConfig.getOriginalName());
                        parameters.add(new Parameter(fieldConfig, value));
                    }
                    break;

                case Modification.DELETE:
                    for (Iterator j=fieldMappings.iterator(); j.hasNext(); ) {
                        FieldMapping fieldMapping = (FieldMapping)j.next();

                        String variable = fieldMapping.getVariable();
                        if (variable == null) continue;

                        if (!variable.equals(attributeName)) continue;
                        
                        String fieldName = fieldMapping.getName();
                        if (debug) log.debug("Setting field "+fieldName+" to null");

                        FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                        columns.add(fieldConfig.getOriginalName());
                        parameters.add(new Parameter(fieldConfig, null));
                    }
                    break;
            }

            interpreter.clear();
        }

        StringBuilder sb = new StringBuilder();
        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = sourceValues.getOne(name);

            int p = name.indexOf(".");
            String sn = name.substring(0, p);
            String fn = name.substring(p+1);

            if (!sourceName.equals(sn)) continue;

            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fn);

            if (sb.length() > 0) sb.append(" and ");

            filterBuilder.generate(
                    fieldConfig,
                    fn,
                    "=",
                    "?",
                    sb
            );

            statement.addParameter(new Parameter(fieldConfig, value));
        }

        String whereClause = sb.toString();
        if (debug) log.debug(" - Where clause: "+whereClause);
        statement.setWhereClause(whereClause);

        if (!columns.isEmpty()) statements.add(statement);
    }

    public void generateSecondaryStatements(
            Collection statements,
            SourceMapping sourceMapping
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        Collection modifications = request.getModifications();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            if (debug) {
                switch (type) {
                    case Modification.ADD:
                        log.debug("Adding attribute "+attributeName+": "+attributeValues);
                        break;
                    case Modification.REPLACE:
                        log.debug("Replacing attribute "+attributeName+": "+attributeValues);
                        break;
                    case Modification.DELETE:
                        log.debug("Deleting attribute "+attributeName+": "+attributeValues);
                        break;
                }
            }

            boolean first = true;

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

                    switch (type) {
                        case Modification.ADD:
                            generateInsertStatement(
                                    statements,
                                    sourceMapping,
                                    values
                            );
                            break;

                        case Modification.REPLACE:
                            if (first) {
                                generateDeleteStatement(
                                        statements,
                                        sourceMapping
                                );
                                first = false;
                            }
                            generateInsertStatement(
                                    statements,
                                    sourceMapping,
                                    values
                            );
                            break;

                        case Modification.DELETE:
                            generateDeleteStatement(
                                    statements,
                                    sourceMapping,
                                    values
                            );
                            break;
                    }
                }

                interpreter.clear();
            }

        }
    }

    public void generateInsertStatement(
            Collection statements,
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

    public void generateDeleteStatement(
            Collection statements,
            SourceMapping sourceMapping
    ) throws Exception {
        generateDeleteStatement(statements, sourceMapping, null);
    }

    public void generateDeleteStatement(
            Collection statements,
            SourceMapping sourceMapping,
            Map values
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Deleting rows from "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        DeleteStatement statement = new DeleteStatement();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        statement.setTable(table);

        StringBuilder sb = new StringBuilder();

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            Object value = sourceValues.getOne(variable);
            if (value == null) continue;

            String fieldName = fieldMapping.getName();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            if (sb.length() > 0) sb.append(" and ");

            filterBuilder.generate(
                    fieldConfig,
                    fieldName,
                    "=",
                    "?",
                    sb
            );

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addParameter(new Parameter(fieldConfig, value));
        }

        if (values != null) {
            for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
                String fieldName = (String)i.next();
                Object value = values.get(fieldName);
                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

                if (sb.length() > 0) sb.append(" and ");

                filterBuilder.generate(
                        fieldConfig,
                        fieldName,
                        "=",
                        "?",
                        sb
                );

                if (debug) log.debug(" - Field: "+fieldName+": "+value);
                statement.addParameter(new Parameter(fieldConfig, value));
            }
        }

        String whereClause = sb.toString();
        if (debug) log.debug(" - Where clause: "+whereClause);
        statement.setWhereClause(whereClause);

        statements.add(statement);
    }
}
