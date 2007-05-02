package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.safehaus.penrose.jdbc.QueryRequest;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SearchRequestBuilder extends RequestBuilder {

    EntryMapping entryMapping;

    Map sourceRefs = new LinkedHashMap(); // need to maintain order
    SourceRef primarySourceRef;

    SourceValues sourceValues;
    Interpreter interpreter;

    SearchRequest request;
    SearchResponse response;

    SearchFilterBuilder filterBuilder;

    public SearchRequestBuilder(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceRefs,
            SourceValues sourceValues,
            Interpreter interpreter,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        this.entryMapping = entryMapping;

        for (Iterator i=sourceRefs.iterator(); i.hasNext(); ) {
            SourceRef sourceRef = (SourceRef)i.next();
            this.sourceRefs.put(sourceRef.getAlias(), sourceRef);
        }

        primarySourceRef = (SourceRef)sourceRefs.iterator().next();

        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;

        filterBuilder = new SearchFilterBuilder(
                entryMapping,
                sourceRefs,
                sourceValues,
                interpreter
        );
    }

    public String generateJoinType(SourceRef sourceRef) {
        String joinType = sourceRef.isRequired() ? "join" : "left join";
        log.debug(" - Join type: "+joinType);
        return joinType;
    }

    public String generateJoinOn(SourceRef sourceRef) {
        return generateJoinOn(sourceRef, sourceRef.getAlias());
    }

    public String generateJoinOn(SourceRef sourceRef, String alias) {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug(" - Join on:");

        StringBuffer sb = new StringBuffer();

        if (primarySourceRef == sourceRef) {

            for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                FieldRef fieldRef = (FieldRef)j.next();

                FieldMapping fieldMapping = fieldRef.getFieldMapping();

                String variable = fieldMapping.getVariable();
                if (variable == null) continue;

                AttributeMapping attributeMapping = entryMapping.getAttributeMapping(variable);
                if (!attributeMapping.isRdn()) continue;

                String sn = primarySourceRef.getAlias();
                String fn = fieldRef.getName();

                SourceRef s = (SourceRef)sourceRefs.get(sn);
                FieldRef f = s.getFieldRef(fn);

                String lhs = alias+"."+ fieldRef.getOriginalName();
                String rhs = sn+"."+f.getOriginalName();

                if (debug) log.debug("   - "+lhs+": "+rhs);

                if (sb.length() > 0) sb.append(" and ");
                sb.append(lhs);
                sb.append("=");
                sb.append(rhs);
            }

        } else {

            for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                FieldRef fieldRef = (FieldRef)j.next();

                FieldMapping fieldMapping = fieldRef.getFieldMapping();

                String variable = fieldMapping.getVariable();
                if (variable == null) continue;

                int p = variable.indexOf(".");
                if (p < 0) continue;

                String sn = variable.substring(0, p);
                String fn = variable.substring(p+1);

                SourceRef s = (SourceRef)sourceRefs.get(sn);
                FieldRef f = s.getFieldRef(fn);

                String lhs = alias+"."+ fieldRef.getOriginalName();
                String rhs = sn+"."+f.getOriginalName();

                if (debug) log.debug("   - "+lhs+": "+rhs);

                if (sb.length() > 0) sb.append(" and ");
                sb.append(lhs);
                sb.append("=");
                sb.append(rhs);
            }
        }

        return sb.toString();
    }

    public QueryRequest generate() throws Exception {

        boolean debug = log.isDebugEnabled();

        SelectStatement statement = new SelectStatement();

        int sourceCounter = 0;
        for (Iterator i= sourceRefs.values().iterator(); i.hasNext(); sourceCounter++) {
            SourceRef sourceRef = (SourceRef)i.next();

            String sourceName = sourceRef.getAlias();
            if (debug) log.debug("Processing source "+sourceName);

            for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                FieldRef fieldRef = (FieldRef)j.next();
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

        Map tableAliases = filterBuilder.getSourceAliases();
        for (Iterator i= tableAliases.keySet().iterator(); i.hasNext(); ) {
            String alias = (String)i.next();
            SourceRef sourceRef = (SourceRef)tableAliases.get(alias);

            if (debug) log.debug("Adding source "+alias);
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
