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
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.jdbc.DeleteStatement;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DeleteStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    JDBCAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    DeleteRequest request;
    DeleteResponse response;

    FilterBuilder filterBuilder;

    Collection statements = new ArrayList();

    public DeleteStatementBuilder(
            JDBCAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
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

        DeleteStatement statement = new DeleteStatement();

        String table = adapter.getTableName(sourceConfig);
        if (debug) log.debug(" - Table: "+table);
        statement.setTable(table);

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

        String whereClause = sb.toString();
        if (debug) log.debug(" - Where clause: "+whereClause);
        statement.setWhereClause(whereClause);

        statements.add(0, statement);
    }
}
