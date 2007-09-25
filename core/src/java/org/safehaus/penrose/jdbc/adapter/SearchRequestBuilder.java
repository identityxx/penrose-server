package org.safehaus.penrose.jdbc.adapter;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.safehaus.penrose.jdbc.QueryRequest;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.safehaus.penrose.jdbc.JoinClause;
import org.safehaus.penrose.directory.FieldRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.partition.Partition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder extends RequestBuilder {

    Partition partition;

    Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // need to maintain order
    SourceRef primarySourceRef;

    SourceValues sourceValues;

    SearchRequest request;
    SearchResponse response;

    SearchFilterBuilder filterBuilder;

    public SearchRequestBuilder(
            Interpreter interpreter,
            Partition partition,
            Collection<SourceRef> primarySourceRefs,
            Collection<SourceRef> localSourceRefs,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        this.partition = partition;

        for (SourceRef sourceRef : sourceRefs) {
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        primarySourceRef = sourceRefs.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        filterBuilder = new SearchFilterBuilder(
                interpreter,
                partition,
                primarySourceRefs,
                localSourceRefs,
                sourceRefs,
                sourceValues
        );
    }

    public String generateJoinType(SourceRef sourceRef) {
        String search = sourceRef.getSearch();
        String joinType = search == null || SourceMapping.SUFFICIENT.equals(search) ? "left join" : "join" ;
        log.debug(" - Join type: "+joinType);
        return joinType;
    }

    public Filter generateJoinOn(SourceRef sourceRef) throws Exception {
        return generateJoinOn(sourceRef, sourceRef.getAlias());
    }

    public String generateJoinFilter(SourceRef sourceRef) throws Exception {
        return generateJoinFilter(sourceRef, sourceRef.getAlias());
    }

    public Filter generateJoinOn(SourceRef sourceRef, String alias) throws Exception {

        if (debug) log.debug(" - Join on:");

        Filter filter = null;

        if (primarySourceRef == sourceRef) {

            // join using fields that are used as RDN
            
            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                if (!fieldRef.isRdn()) continue;

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

            // join using fields that are mapped to another source using variable mapping

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {

                String variable = fieldRef.getVariable();
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

    public String generateJoinFilter(SourceRef sourceRef, String alias) throws Exception {

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
            StringBuilder sb = new StringBuilder();

            sb.append("(");
            sb.append(sourceFilter);
            sb.append(") and (");
            sb.append(sourceMappingFilter);
            sb.append(")");

            return sb.toString();
        }
    }

    public QueryRequest generate() throws Exception {

        SelectStatement statement = new SelectStatement();

        int sourceCounter = 0;
        Collection<String> filters = new ArrayList<String>();
        for (SourceRef sourceRef : sourceRefs.values()) {

            String alias = sourceRef.getAlias();
            if (debug) log.debug("Processing source " + alias);

            for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                if (debug) log.debug(" - field " + fieldRef.getName());
                statement.addFieldRef(fieldRef);
            }

            statement.addSourceRef(sourceRef);

            if (sourceCounter == 0) { // join previous source
                String where = sourceRef.getParameter(JDBCClient.FILTER);
                if (where != null) filters.add(where);

                Source source = sourceRef.getSource();
                where = source.getParameter(JDBCClient.FILTER);
                if (where != null) {
                    where = where.replaceAll(source.getName()+".", alias+".");
                    filters.add(where);
                }
                
            } else { // add filter of first source
                String joinType = generateJoinType(sourceRef);
                Filter joinCondition = generateJoinOn(sourceRef);
                String joinWhere = generateJoinFilter(sourceRef);

                JoinClause joinClause = new JoinClause();
                joinClause.setType(joinType);
                joinClause.setCondition(joinCondition);
                joinClause.setWhere(joinWhere);

                statement.addJoin(joinClause);
            }

            Collection<FieldRef> primaryKeyFieldRefs = sourceRef.getPrimaryKeyFieldRefs();
            if (debug) log.debug("Order by: "+primaryKeyFieldRefs);

            statement.addOrders(primaryKeyFieldRefs);
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
            Filter joinCondition = generateJoinOn(sourceRef, alias);
            String joinWhere = generateJoinFilter(sourceRef, alias);

            JoinClause joinClause = new JoinClause();
            joinClause.setType(joinType);
            joinClause.setCondition(joinCondition);
            joinClause.setWhere(joinWhere);

            statement.addJoin(joinClause);
        }

        Filter sourceFilter = filterBuilder.getFilter();
        if (debug) log.debug("Source filter: "+sourceFilter);

        statement.setFilter(sourceFilter);

        if (!filters.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String where : filters) {
                if (sb.length() > 0) sb.append(" and ");
                sb.append("(");
                sb.append(where);
                sb.append(")");
            }
            statement.setWhere(sb.toString());
        }

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setStatement(statement);

        requests.add(queryRequest);

        return queryRequest;
    }

}
