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
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCQueryGenerator {

    Logger log = LoggerFactory.getLogger(getClass());

    protected Collection fields = new ArrayList();
    protected Collection tables = new ArrayList();
    protected Collection joinTypes = new ArrayList();
    protected Collection joinOns = new ArrayList();
    protected Collection filters = new ArrayList();
    protected Collection orders = new ArrayList();

    protected List parameterValues = new ArrayList();
    protected List parameterFieldCofigs = new ArrayList();

    protected String sql;

    JDBCAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;
    Map sourceMappings = new HashMap();

    AttributeValues sourceValues;
    Interpreter interpreter;

    SearchRequest request;
    SearchResponse response;

    JDBCFilterGenerator filterGenerator;

    public JDBCQueryGenerator(
            JDBCAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            Interpreter interpreter,
            SearchRequest request,
            SearchResponse response
    ) {
        this.adapter = adapter;

        this.partition = partition;
        this.entryMapping = entryMapping;

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            this.sourceMappings.put(sourceMapping.getName(), sourceMapping);
        }

        this.sourceValues = sourceValues;
        this.interpreter = interpreter;

        this.request = request;
        this.response = response;

        filterGenerator = new JDBCFilterGenerator(
                partition,
                entryMapping,
                sourceMappings,
                sourceValues,
                interpreter,
                parameterValues,
                parameterFieldCofigs,
                request.getFilter()
        );

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

    public void run() throws Exception {

        boolean debug = log.isDebugEnabled();

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

            String table = adapter.getTableName(sourceConfig);
            if (debug) log.debug(" - Table: "+table);
            tables.add(table+" "+sourceName);

            // join previous table
            if (sourceCounter > 0) {
                String joinType = generateJoinType(sourceMapping);
                if (debug) log.debug(" - Join type: "+joinType);
                joinTypes.add(joinType);

                String joinOn = generateJoinOn(sourceMapping);
                if (debug) log.debug(" - Join on: "+joinOn);
                joinOns.add(joinOn);
            }

            Collection nonPkfieldConfigs = sourceConfig.getFieldConfigs();
            for (Iterator j=nonPkfieldConfigs.iterator(); j.hasNext(); ) {
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
        filterGenerator.generate();

        Map tableAliases = filterGenerator.getTableAliases();
        for (Iterator i= tableAliases.keySet().iterator(); i.hasNext(); ) {
            String alias = (String)i.next();

            String sourceName = (String)tableAliases.get(alias);
            if (debug) log.debug("Adding source "+sourceName);

            SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceName);
            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            String table = adapter.getTableName(sourceConfig);
            if (debug) log.debug(" - Table: "+table);
            tables.add(table+" "+alias);

            String joinType = generateJoinType(sourceMapping);
            if (debug) log.debug(" - Join type: "+joinType);
            joinTypes.add(joinType);

            String joinOn = generateJoinOn(sourceMapping, alias);
            if (debug) log.debug(" - Join on: "+joinOn);
            joinOns.add(joinOn);
        }

        String sqlFilter = filterGenerator.getJdbcFilter();
        if (sqlFilter.length() > 0) {
            if (debug) log.debug("SQL filter: "+sqlFilter);
            filters.add(sqlFilter);
        }

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

        StringBuilder sb = new StringBuilder();
        sb.append("select distinct ");
        sb.append(toList(fields));

        sb.append(" from ");

        Iterator i=tables.iterator();
        sb.append(i.next());

        for (Iterator j= joinTypes.iterator(), k= joinOns.iterator(); i.hasNext() && j.hasNext() && k.hasNext(); ) {
            String table = (String)i.next();
            String joinType = (String)j.next();
            String joinOn = (String)k.next();
            sb.append(" ");
            sb.append(joinType);
            sb.append(" ");
            sb.append(table);
            sb.append(" on ");
            sb.append(joinOn);
        }

        if (filters.size() > 0) {
            sb.append(" where ");
            sb.append(toFilter(filters));
        }

        sb.append(" order by ");
        sb.append(toList(orders));

/*
        int totalCount = response.getTotalCount();
        long sizeLimit = request.getSizeLimit();

        if (sizeLimit == 0) {
            log.debug("Retrieving all entries.");

        } else {
            int size = sizeLimit - totalCount + 1;
            if (debug) log.debug("Retrieving "+size+" entries.");

            sb.append(" limit ");
            sb.append(size);
        }
*/

        sql = sb.toString();
    }

    public String toList(Collection list) throws Exception {
        StringBuilder sb = new StringBuilder();

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Object object = i.next();

            if (sb.length() > 0) sb.append(", ");
            sb.append(object.toString());
        }

        return sb.toString();
    }

    public String toFilter(Collection list) throws Exception {
        StringBuilder sb = new StringBuilder();

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Object object = i.next();

            if (sb.length() > 0) sb.append(" and ");
            sb.append(object.toString());
        }

        return sb.toString();
    }

    public Collection getFields() {
        return fields;
    }

    public void setFields(Collection fields) {
        this.fields = fields;
    }

    public Collection getTables() {
        return tables;
    }

    public void setTables(Collection tables) {
        this.tables = tables;
    }

    public Collection getJoinTypes() {
        return joinTypes;
    }

    public void setJoinTypes(Collection joinTypes) {
        this.joinTypes = joinTypes;
    }

    public Collection getJoinOns() {
        return joinOns;
    }

    public void setJoinOns(Collection joinOns) {
        this.joinOns = joinOns;
    }

    public Collection getFilters() {
        return filters;
    }

    public void setFilters(Collection filters) {
        this.filters = filters;
    }

    public List getParameterValues() {
        return parameterValues;
    }

    public void setParameterValues(List parameterValues) {
        this.parameterValues = parameterValues;
    }

    public List getParameterFieldCofigs() {
        return parameterFieldCofigs;
    }

    public void setParameterFieldCofigs(List parameterFieldCofigs) {
        this.parameterFieldCofigs = parameterFieldCofigs;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Collection getOrders() {
        return orders;
    }

    public void setOrders(Collection orders) {
        this.orders = orders;
    }
}
