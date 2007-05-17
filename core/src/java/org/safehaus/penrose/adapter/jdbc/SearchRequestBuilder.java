package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.safehaus.penrose.jdbc.QueryRequest;
import org.safehaus.penrose.jdbc.JDBCClient;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder extends RequestBuilder {

    PenroseContext penroseContext;

    EntryMapping entryMapping;

    Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // need to maintain order
    SourceRef primarySourceRef;

    SourceValues sourceValues;

    SearchRequest request;
    SearchResponse<SearchResult> response;

    SearchFilterBuilder filterBuilder;

    public SearchRequestBuilder(
            PenroseContext penroseContext,
            Partition partition,
            EntryMapping entryMapping,
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse<SearchResult> response
    ) throws Exception {

        this.penroseContext = penroseContext;

        this.entryMapping = entryMapping;

        for (SourceRef sourceRef : sourceRefs) {
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        primarySourceRef = sourceRefs.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        filterBuilder = new SearchFilterBuilder(
                penroseContext,
                partition,
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

    public String generateJoinOn(SourceRef sourceRef) throws Exception {
        return generateJoinOn(sourceRef, sourceRef.getAlias());
    }

    public String generateJoinOn(SourceRef sourceRef, String alias) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug(" - Join on:");

        String table = sourceRef.getSource().getParameter(JDBCClient.TABLE);
        String sourceFilter = sourceRef.getSource().getParameter(JDBCClient.FILTER);
        String sourceMappingFilter = sourceRef.getParameter(JDBCClient.FILTER);

        StringBuffer sb = new StringBuffer();

        if (sourceFilter != null && sourceMappingFilter != null) {
            sb.append("(");
        }

        if (primarySourceRef == sourceRef) {

            boolean first = true;

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

                if (debug) log.debug("   - " + lhs + ": " + rhs);

                if (first) {
                    first = false;
                } else {
                    sb.append(" and ");
                }

                sb.append(lhs);
                sb.append("=");
                sb.append(rhs);
            }

        } else {

            boolean first = true;
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

                if (debug) log.debug("   - " + lhs + ": " + rhs);

                if (first) {
                    first = false;
                } else {
                    sb.append(" and ");
                }
                
                sb.append(lhs);
                sb.append("=");
                sb.append(rhs);
            }
        }

        if (sourceFilter != null && sourceMappingFilter != null) {
            sb.append(")");
        }

        if (sourceFilter != null) {
            sourceFilter = sourceFilter.replaceAll(table+"\\.", alias+"\\.");
            if (debug) log.debug(" - Source filter: "+sourceFilter);

            sb.append(" and (");
            sb.append(sourceFilter);
            sb.append(")");
        }

        if (sourceMappingFilter != null) {
            sourceMappingFilter = sourceMappingFilter.replaceAll(sourceRef.getAlias()+"\\.", alias+"\\.");
            if (debug) log.debug(" - Source mapping filter: "+sourceMappingFilter);

            sb.append(" and (");
            sb.append(sourceMappingFilter);
            sb.append(")");
        }

        return sb.toString();
    }

    public QueryRequest generate() throws Exception {

        boolean debug = log.isDebugEnabled();

        SelectStatement statement = new SelectStatement();

        int sourceCounter = 0;
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
                String joinOn = generateJoinOn(sourceRef);

                statement.addJoin(joinType, joinOn);
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
            String joinOn = generateJoinOn(sourceRef, alias);

            statement.addJoin(joinType, joinOn);
        }

        Filter sourceFilter = filterBuilder.getFilter();
        if (debug) log.debug("Source filter: "+sourceFilter);

        statement.setFilter(sourceFilter);

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
