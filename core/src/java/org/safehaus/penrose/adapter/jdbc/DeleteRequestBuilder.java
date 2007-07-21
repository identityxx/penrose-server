package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.DeleteStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.jdbc.Request;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.DeleteRequest;
import org.safehaus.penrose.ldap.DeleteResponse;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class DeleteRequestBuilder extends RequestBuilder {

    Collection<SourceRef> sourceRefs;

    SourceValues sourceValues;
    Interpreter interpreter;

    DeleteRequest request;
    DeleteResponse response;

    public DeleteRequestBuilder(
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            Interpreter interpreter,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        this.sourceRefs = sourceRefs;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection<Request> generate() throws Exception {

        boolean first = true;
        for (SourceRef sourceRef : sourceRefs) {

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

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        DeleteStatement statement = new DeleteStatement();

        Source source = sourceRef.getSource();
        statement.setSource(source);

        Filter filter = null;

        Attributes values = sourceValues.get(sourceName);

        for (String fieldName : values.getNames()) {
            if (fieldName.startsWith("primaryKey.")) continue;

            Object value = values.getValue(fieldName);

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
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

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        DeleteStatement statement = new DeleteStatement();

        Source source = sourceRef.getSource();
        statement.setSource(source);

        Filter filter = null;

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf(".");
            String sn = variable.substring(0, i);
            String fn = variable.substring(i + 1);

            Attributes fields = sourceValues.get(sn);
            if (fields == null) continue;

            Object value = fields.getValue(fn);
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter(fieldName, "=", "?");
            filter = FilterTool.appendAndFilter(filter, sf);

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
        }

        statement.setFilter(filter);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(0, updateRequest);
    }
}
