package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.safehaus.penrose.jdbc.QueryRequest;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.safehaus.penrose.jdbc.JoinClause;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder extends RequestBuilder {

    EntryMapping entryMapping;

    Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // need to maintain order
    SourceRef primarySourceRef;

    SourceValues sourceValues;

    SearchRequest request;
    SearchResponse<SearchResult> response;

    SearchFilterBuilder filterBuilder;

    public SearchRequestBuilder(
            Interpreter interpreter,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        this.entryMapping = entryMapping;

        for (SourceRef sourceRef : sourceRefs) {
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        primarySourceRef = sourceRefs.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        filterBuilder = new SearchFilterBuilder(
                interpreter,
                entryMapping,
                sourceRefs,
                sourceValues
        );
    }

    public String generateJoinType(SourceRef sourceRef) {
        String joinType = sourceRef.isRequired() ? "join" : "left join";
        log.debug(" - Join type: "+joinType);
        return joinType;
    }

    public Filter generateJoinFilter(SourceRef sourceRef) throws Exception {
        return generateJoinFilter(sourceRef, sourceRef.getAlias());
    }

    public String generateJoinOn(SourceRef sourceRef) throws Exception {
        return generateJoinOn(sourceRef, sourceRef.getAlias());
    }

    public Filter generateJoinFilter(SourceRef sourceRef, String alias) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug(" - Join on:");

        Filter filter = null;

        if (primarySourceRef == sourceRef) {

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                FieldMapping fieldMapping = fieldRef.getFieldMapping();

                String variable = fieldMapping.getVariable();
                if (variable == null) continue;

                AttributeMapping attributeMapping = entryMapping.getAttributeMapping(variable);
                if (attributeMapping == null) throw new Exception("Unknown attribute "+variable);

                if (!attributeMapping.isRdn()) continue;

                String sn = primarySourceRef.getAlias();
                String fn = fieldRef.getName();

                SourceRef s = sourceRefs.get(sn);
                FieldRef f = s.getFieldRef(fn);

                String lhs = alias + "." + fieldRef.getOriginalName();
                String rhs = sn + "." + f.getOriginalName();

                if (debug) log.debug("   - " + lhs + " =  " + rhs);

                SimpleFilter sf = new SimpleFilter(lhs, "=", rhs);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

        } else {

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                FieldMapping fieldMapping = fieldRef.getFieldMapping();

                String variable = fieldMapping.getVariable();
                if (variable == null) continue;

                int p = variable.indexOf(".");
                if (p < 0) continue;

                String sn = variable.substring(0, p);
                String fn = variable.substring(p + 1);

                SourceRef s = sourceRefs.get(sn);
                FieldRef f = s.getFieldRef(fn);

                String lhs = alias + "." + fieldRef.getOriginalName();
                String rhs = sn + "." + f.getOriginalName();

                if (debug) log.debug("   - " + lhs + " = " + rhs);

                SimpleFilter sf = new SimpleFilter(lhs, "=", rhs);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
        }

        return filter;
    }

    public String generateJoinOn(SourceRef sourceRef, String alias) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug(" - Join filter:");

        String table = sourceRef.getSource().getParameter(JDBCClient.TABLE);

        String sourceFilter = sourceRef.getSource().getParameter(JDBCClient.FILTER);
        String sourceMappingFilter = sourceRef.getParameter(JDBCClient.FILTER);

        if (sourceFilter == null && sourceMappingFilter == null) {
            return null;
        }

        if (sourceFilter != null) {
            sourceFilter = sourceFilter.replaceAll(table+"\\.", alias+"\\.");
            if (debug) log.debug("   - "+sourceFilter);
        }

        if (sourceMappingFilter != null) {
            sourceMappingFilter = sourceMappingFilter.replaceAll(sourceRef.getAlias()+"\\.", alias+"\\.");
            if (debug) log.debug("   - "+sourceMappingFilter);
        }

        if (sourceMappingFilter == null) {
            return sourceFilter;

        } else if (sourceFilter == null) {
            return sourceMappingFilter;

        } else {
            StringBuffer sb = new StringBuffer();

            sb.append("(");
            sb.append(sourceFilter);
            sb.append(") and (");
            sb.append(sourceMappingFilter);
            sb.append(")");

            return sb.toString();
        }
    }

    public QueryRequest generate() throws Exception {

        boolean debug = log.isDebugEnabled();

        SelectStatement statement = new SelectStatement();

        int sourceCounter = 0;
        String where = null;
        for (SourceRef sourceRef : sourceRefs.values()) {

            String sourceName = sourceRef.getAlias();
            if (debug) log.debug("Processing source " + sourceName);

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                statement.addFieldRef(fieldRef);
            }

            statement.addSourceRef(sourceRef);

            // join previous table
            if (sourceCounter > 0) {
                String joinType = generateJoinType(sourceRef);
                Filter joinFilter = generateJoinFilter(sourceRef);
                String joinOn = generateJoinOn(sourceRef);

                JoinClause joinClause = new JoinClause();
                joinClause.setType(joinType);
                joinClause.setFilter(joinFilter);
                joinClause.setSql(joinOn);

                statement.addJoin(joinClause);

            } else {
                where = sourceRef.getParameter(JDBCClient.FILTER);
            }

            statement.addOrders(sourceRef.getPrimaryKeyFieldRefs());
            sourceCounter++;
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

        Map<String,SourceRef> tableAliases = filterBuilder.getSourceAliases();
        for (String alias : tableAliases.keySet()) {
            SourceRef sourceRef = tableAliases.get(alias);

            if (debug) log.debug("Adding source " + alias);
            statement.addSourceRef(alias, sourceRef);

            String joinType = generateJoinType(sourceRef);
            Filter joinFilter = generateJoinFilter(sourceRef, alias);
            String joinOn = generateJoinOn(sourceRef, alias);

            JoinClause joinClause = new JoinClause();
            joinClause.setType(joinType);
            joinClause.setFilter(joinFilter);
            joinClause.setSql(joinOn);

            statement.addJoin(joinClause);
        }

        Filter sourceFilter = filterBuilder.getFilter();
        if (debug) log.debug("Source filter: "+sourceFilter);

        statement.setFilter(sourceFilter);
        statement.setWhere(where);

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

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setStatement(statement);

        requests.add(queryRequest);

        return queryRequest;
    }

}
