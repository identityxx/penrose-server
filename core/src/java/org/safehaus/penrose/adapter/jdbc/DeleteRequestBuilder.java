package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.DeleteStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.DeleteRequest;
import org.safehaus.penrose.ldap.DeleteResponse;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DeleteRequestBuilder extends RequestBuilder {

    Collection sources;

    AttributeValues sourceValues;
    Interpreter interpreter;

    DeleteRequest request;
    DeleteResponse response;

    public DeleteRequestBuilder(
            Collection sources,
            AttributeValues sourceValues,
            Interpreter interpreter,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        this.sources = sources;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection generate() throws Exception {

        boolean first = true;
        for (Iterator i= sources.iterator(); i.hasNext(); ) {
            SourceRef sourceRef = (SourceRef)i.next();

            if (first) {
                generatePrimaryRequest(sourceRef);
                first = false;
                
            } else {
                generateSecondaryRequests(sourceRef);
            }
        }

        return requests;
    }

    public void generatePrimaryRequest(
            SourceRef sourceRef
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        DeleteStatement statement = new DeleteStatement();

        Source source = sourceRef.getSource();
        statement.setSource(source);

        Filter filter = null;

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = sourceValues.getOne(name);

            int p = name.indexOf(".");
            String sn = name.substring(0, p);
            String fn = name.substring(p+1);

            if (!sourceName.equals(sn)) continue;

            FieldRef fieldRef = sourceRef.getFieldRef(fn);
            Field field = fieldRef.getField();

            SimpleFilter sf = new SimpleFilter(fn, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        statement.setFilter(filter);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(updateRequest);
    }

    public void generateSecondaryRequests(
            SourceRef sourceRef
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        DeleteStatement statement = new DeleteStatement();

        Source source = sourceRef.getSource();
        statement.setSource(source);

        Filter filter = null;

        for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)k.next();
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            Object value = sourceValues.getOne(variable);
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter(fieldName, "=", "?");
            filter = FilterTool.appendAndFilter(filter, sf);

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
        }

        statement.setFilter(filter);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(0, updateRequest);
    }
}
