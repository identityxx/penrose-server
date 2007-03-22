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
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.ModRdnRequest;
import org.safehaus.penrose.session.ModRdnResponse;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.jdbc.UpdateStatement;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModRdnStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    JDBCAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    ModRdnRequest request;
    ModRdnResponse response;

    FilterBuilder filterBuilder;

    AttributeValues newSourceValues = new AttributeValues();
    Collection statements = new ArrayList();

    public ModRdnStatementBuilder(
            JDBCAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
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
                sourceValues,
                interpreter
        );
    }

    public Collection generate() throws Exception {

        List statements = new ArrayList();

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
            List statements,
            SourceMapping sourceMapping
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        UpdateStatement statement = new UpdateStatement();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        statement.addTable(table);

        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        newSourceValues.set(sourceValues);

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

            newSourceValues.set(sourceName+"."+fieldName, value);
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

        interpreter.clear();

        statements.add(statement);
    }

    public void generateSecondaryStatements(
            List statements,
            SourceMapping sourceMapping
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        UpdateStatement statement = new UpdateStatement();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        statement.addTable(table);

        interpreter.set(newSourceValues);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = newRdn.get(attributeName);

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

        StringBuilder sb = new StringBuilder();
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

        String whereClause = sb.toString();
        if (debug) log.debug(" - Where clause: "+whereClause);
        statement.setWhereClause(whereClause);

        interpreter.clear();

        statements.add(0, statement);
    }
}
