package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchStatementBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    JDBCAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;
    Map sourceMappings = new LinkedHashMap(); // need to maintain order

    AttributeValues sourceValues;
    Interpreter interpreter;

    SearchRequest request;
    SearchResponse response;

    FilterBuilder filterBuilder;

    public SearchStatementBuilder(
            JDBCAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {
        
        this.adapter = adapter;

        this.partition = partition;
        this.entryMapping = entryMapping;

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            this.sourceMappings.put(sourceMapping.getName(), sourceMapping);
        }

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

    public String generateTableAlias(SourceConfig sourceConfig, String alias) {
        return adapter.getTableName(sourceConfig)+" "+alias;
    }

    public String generateJoinType(SourceMapping sourceMapping) {
        if (sourceMapping.isRequired()) return "join";
        return "left join";
    }

    public String generateJoinOn(SourceMapping sourceMapping) {
        return generateJoinOn(sourceMapping, sourceMapping.getName());
    }

    public String generateJoinOn(SourceMapping sourceMapping, String alias) {

        boolean debug = log.isDebugEnabled();

        StringBuffer sb = new StringBuffer();

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        if (debug) log.debug(" - Foreign keys:");
        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator j=fieldMappings.iterator(); j.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)j.next();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            int p = variable.indexOf(".");
            if (p < 0) continue;

            String fieldName = fieldMapping.getName();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
            String lhs = alias+"."+fieldConfig.getOriginalName();

            String sn = variable.substring(0, p);
            String fn = variable.substring(p+1);

            SourceMapping sm = entryMapping.getSourceMapping(sn);
            SourceConfig sc = partition.getSourceConfig(sm);
            FieldConfig fc = sc.getFieldConfig(fn);
            String rhs = sn+"."+fc.getOriginalName();

            if (debug) log.debug("   - "+lhs+": "+rhs);

            if (sb.length() > 0) sb.append(" and ");
            sb.append(lhs);
            sb.append("=");
            sb.append(rhs);
        }

        return sb.toString();
    }

    public SelectStatement generate() throws Exception {

        boolean debug = log.isDebugEnabled();

        SelectStatement statement = new SelectStatement();

        Collection fields = statement.getFields();
        Collection tables = statement.getTables();
        Collection joinTypes = statement.getJoinTypes();
        Collection joinOns = statement.getJoinOns();
        Collection orders = statement.getOrders();
        Collection filters = statement.getFilters();

        int sourceCounter = 0;
        for (Iterator i=sourceMappings.values().iterator(); i.hasNext(); sourceCounter++) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            String sourceName = sourceMapping.getName();
            if (debug) log.debug("Processing source "+sourceName);

            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            Collection fieldConfigs = sourceConfig.getFieldConfigs();
            for (Iterator j=fieldConfigs.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                fields.add(sourceName+"."+fieldConfig.getOriginalName());
            }

            String table = generateTableAlias(sourceConfig, sourceName);
            if (debug) log.debug(" - Table: "+table);
            tables.add(table);

            // join previous table
            if (sourceCounter > 0) {
                String joinType = generateJoinType(sourceMapping);
                if (debug) log.debug(" - Join type: "+joinType);
                joinTypes.add(joinType);

                String joinOn = generateJoinOn(sourceMapping);
                if (debug) log.debug(" - Join on: "+joinOn);
                joinOns.add(joinOn);
            }

            Collection nonPkFieldConfigs = sourceConfig.getFieldConfigs();
            for (Iterator j=nonPkFieldConfigs.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                orders.add(sourceName+"."+fieldConfig.getOriginalName());
            }
        }
/*
        for (Iterator i=entryMapping.getRelationships().iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String leftSource = relationship.getLeftSource();
            String rightSource = relationship.getRightSource();

            if (!sourceMappings.containsKey(leftSource) || !sourceMappings.containsKey(rightSource)) continue;
            joinOns.add(relationship.getExpression());
        }
*/
        filterBuilder.append(request.getFilter());

        Map tableAliases = filterBuilder.getTableAliases();
        for (Iterator i= tableAliases.keySet().iterator(); i.hasNext(); ) {
            String alias = (String)i.next();

            String sourceName = (String)tableAliases.get(alias);
            if (debug) log.debug("Adding source "+sourceName);

            SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            String table = generateTableAlias(sourceConfig, alias);
            if (debug) log.debug(" - Table: "+table);
            tables.add(table);

            String joinType = generateJoinType(sourceMapping);
            if (debug) log.debug(" - Join type: "+joinType);
            joinTypes.add(joinType);

            String joinOn = generateJoinOn(sourceMapping, alias);
            if (debug) log.debug(" - Join on: "+joinOn);
            joinOns.add(joinOn);
        }

        String sqlFilter = filterBuilder.generate();
        if (sqlFilter.length() > 0) {
            if (debug) log.debug("SQL filter: "+sqlFilter);
            filters.add(sqlFilter);
        }

        statement.addParameters(filterBuilder.getParameters());

/*
        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            String defaultFilter = sourceConfig.getParameter(FILTER);

            if (defaultFilter != null) {
                if (debug) log.debug("Default filter: "+defaultFilter);
                filters.add(defaultFilter);
            }
        }
*/
        return statement;
    }

}
