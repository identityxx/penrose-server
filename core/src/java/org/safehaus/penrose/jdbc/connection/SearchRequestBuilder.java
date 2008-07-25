package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SourceAttributes;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.safehaus.penrose.jdbc.JoinClause;
import org.safehaus.penrose.jdbc.source.JDBCSource;
import org.safehaus.penrose.directory.EntryField;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.partition.Partition;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder extends RequestBuilder {

    Partition partition;

    Map<String, EntrySource> sourceRefs = new LinkedHashMap<String, EntrySource>(); // need to maintain order

    SourceAttributes sourceValues;

    SearchRequest request;
    SearchResponse response;

    SearchFilterBuilder filterBuilder;

    public SearchRequestBuilder(
            Partition partition,
            Collection<EntrySource> localSourceRefs,
            Collection<EntrySource> sourceRefs,
            SourceAttributes sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        this.partition = partition;

        for (EntrySource sourceRef : sourceRefs) {
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        filterBuilder = new SearchFilterBuilder(
                partition,
                localSourceRefs,
                sourceRefs,
                sourceValues
        );
    }

    public String generateJoinType(EntrySource sourceRef) {
        String search = sourceRef.getSearch();
        String joinType = search == null || EntrySourceConfig.SUFFICIENT.equals(search) ? "left join" : "join" ;
        log.debug(" - Join type: "+joinType);
        return joinType;
    }

    public Filter generateJoinOn(EntrySource sourceRef, String alias) throws Exception {

        if (debug) log.debug(" - Join on:");

        Filter filter = null;

        // join using fields that are mapped to another source using variable mapping

        for (EntryField fieldRef : sourceRef.getFields()) {

            String variable = fieldRef.getVariable();
            if (variable == null) continue;

            int p = variable.indexOf(".");
            if (p < 0) continue;

            String sn = variable.substring(0, p);
            String fn = variable.substring(p + 1);

            EntrySource s = sourceRefs.get(sn);
            EntryField f = s.getField(fn);

            String lhs = alias + "." + fieldRef.getOriginalName();
            String rhs = sn + "." + f.getOriginalName();

            if (debug) log.debug("   - " + lhs + " = " + rhs);

            SimpleFilter sf = new SimpleFilter(lhs, "=", rhs);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        return filter;
    }

    public Filter generateJoinOn2(EntrySource sourceRef, String alias) throws Exception {

        if (debug) log.debug(" - Join on:");

        Filter filter = null;

        //if (primarySourceRef == sourceRef) {
        if (sourceRef.isPrimarySourceRef()) {

            // join using fields that are used as RDN

            for (EntryField fieldRef : sourceRef.getFields()) {
                if (!fieldRef.isPrimaryKey()) continue;

                String sn = sourceRef.getAlias();
                String fn = fieldRef.getName();

                EntrySource s = sourceRefs.get(sn);
                EntryField f = s.getField(fn);

                String lhs = alias + "." + fieldRef.getOriginalName();
                String rhs = sn + "." + f.getOriginalName();

                if (debug) log.debug("   - " + lhs + " =  " + rhs);

                SimpleFilter sf = new SimpleFilter(lhs, "=", rhs);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

        } else {

            // join using fields that are mapped to another source using variable mapping

            for (EntryField fieldRef : sourceRef.getFields()) {

                String variable = fieldRef.getVariable();
                if (variable == null) continue;

                int p = variable.indexOf(".");
                if (p < 0) continue;

                String sn = variable.substring(0, p);
                String fn = variable.substring(p + 1);

                EntrySource s = sourceRefs.get(sn);
                if (s == null) continue;
                
                EntryField f = s.getField(fn);

                String lhs = alias + "." + fieldRef.getOriginalName();
                String rhs = sn + "." + f.getOriginalName();

                if (debug) log.debug("   - " + lhs + " = " + rhs);

                SimpleFilter sf = new SimpleFilter(lhs, "=", rhs);
                filter = FilterTool.appendAndFilter(filter, sf);
            }
        }

        return filter;
    }

    public String generateJoinFilter(EntrySource sourceRef) throws Exception {
        return generateJoinFilter(sourceRef, sourceRef.getAlias());
    }

    public String generateJoinFilter(EntrySource sourceRef, String alias) throws Exception {

        if (debug) log.debug(" - Join filter:");

        String table = sourceRef.getSource().getParameter(JDBCSource.TABLE);

        String sourceFilter = sourceRef.getSource().getParameter(JDBCSource.FILTER);
        String sourceMappingFilter = sourceRef.getParameter(JDBCSource.FILTER);

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

    public SelectStatement generate() throws Exception {

        SelectStatement statement = new SelectStatement();

        int sourceCounter = 0;
        Collection<String> filters = new ArrayList<String>();
        for (EntrySource sourceRef : sourceRefs.values()) {

            String alias = sourceRef.getAlias();
            if (debug) log.debug("Processing source " + alias);

            for (EntryField fieldRef : sourceRef.getFields()) {
                if (debug) log.debug(" - field " + fieldRef.getName());
                statement.addColumn(fieldRef.getSourceName()+"."+fieldRef.getOriginalName());
            }

            statement.addSourceName(sourceRef.getAlias(), sourceRef.getSource().getPartition().getName(), sourceRef.getSource().getName());

            if (sourceCounter == 0) { // join previous source
                String where = sourceRef.getParameter(JDBCSource.FILTER);
                if (where != null) filters.add(where);

                Source source = sourceRef.getSource();
                where = source.getParameter(JDBCSource.FILTER);
                if (where != null) {
                    where = where.replaceAll(source.getName()+".", alias+".");
                    filters.add(where);
                }
                
            } else { // add filter of first source
                String joinType = generateJoinType(sourceRef);
                Filter joinCondition = generateJoinOn(sourceRef, alias);
                String joinWhere = generateJoinFilter(sourceRef);

                JoinClause joinClause = new JoinClause();
                joinClause.setType(joinType);
                joinClause.setCondition(joinCondition);
                joinClause.setWhere(joinWhere);

                statement.addJoin(joinClause);
            }

            Collection<EntryField> primaryKeyFieldRefs = sourceRef.getPrimaryKeyFields();
            if (debug) log.debug("Order by: "+primaryKeyFieldRefs);

            for (EntryField fieldRef : primaryKeyFieldRefs) {
                statement.addOrder(fieldRef.getSourceName()+"."+fieldRef.getOriginalName());
            }

            sourceCounter++;
        }
/*
        for (Iterator i=entryConfig.getRelationships().iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String leftSource = relationship.getLeftSource();
            String rightSource = relationship.getRightSource();

            if (!sourceMappings.containsKey(leftSource) || !sourceMappings.containsKey(rightSource)) continue;
            joinOns.add(relationship.getExpression());
        }
*/
        filterBuilder.append(request.getFilter());

        Map<String, EntrySource> tableAliases = filterBuilder.getSourceAliases();
        for (String alias : tableAliases.keySet()) {
            EntrySource sourceRef = tableAliases.get(alias);

            if (debug) log.debug("Adding source " + alias);
            statement.addSourceName(alias, sourceRef.getSource().getPartition().getName(), sourceRef.getSource().getName());

            String joinType = generateJoinType(sourceRef);
            Filter joinCondition = generateJoinOn2(sourceRef, alias);
            if (joinCondition == null) continue;
            
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

        requests.add(statement);

        return statement;
    }

}
