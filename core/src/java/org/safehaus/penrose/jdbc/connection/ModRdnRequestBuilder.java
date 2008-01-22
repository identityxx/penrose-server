package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.FieldRef;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.jdbc.UpdateStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.jdbc.Assignment;
import org.safehaus.penrose.jdbc.Request;
import org.safehaus.penrose.source.*;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModRdnRequestBuilder extends RequestBuilder {

    Collection sources;

    SourceValues sourceValues;
    Interpreter interpreter;

    ModRdnRequest request;
    ModRdnResponse response;

    SourceValues newSourceValues = new SourceValues();

    public ModRdnRequestBuilder(
            Collection sources,
            SourceValues sourceValues,
            Interpreter interpreter,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        this.sources = sources;
        this.sourceValues = sourceValues;
        newSourceValues = (SourceValues)sourceValues.clone();

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection<Request> generate() throws Exception {

        int sourceCounter = 0;
        for (Iterator i= sources.iterator(); i.hasNext(); sourceCounter++) {
            SourceRef sourceRef = (SourceRef)i.next();

            if (sourceCounter == 0) {
                generatePrimaryRequest(sourceRef);
            } else {
                generateSecondaryRequests(sourceRef);
            }
        }

        return requests;
    }

    public void generatePrimaryRequest(
            SourceRef sourceRef
    ) throws Exception {

        String alias = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+alias);

        UpdateStatement statement = new UpdateStatement();

        statement.setSourceRef(sourceRef);

        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (String attributeName : newRdn.getNames()) {
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        Attributes attributes = newSourceValues.get(alias);

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            Object value = interpreter.eval(fieldRef);
            if (value == null) continue;

            if ("INTEGER".equals(field.getType()) && value instanceof String) {
                value = Integer.parseInt((String)value);
            }

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef, value));

            attributes.setValue(fieldName, value);
        }

        Filter filter = null;

        attributes = sourceValues.get(alias);
        
        for (String fieldName : attributes.getNames()) {

            Object value = attributes.getValue(fieldName);

            FieldRef fieldRef = sourceRef.getFieldRef(fieldName);

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        statement.setFilter(filter);

        interpreter.clear();

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(updateRequest);
    }

    public void generateSecondaryRequests(
            SourceRef sourceRef
    ) throws Exception {

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        UpdateStatement statement = new UpdateStatement();

        statement.setSourceRef(sourceRef);

        interpreter.set(newSourceValues);

        RDN newRdn = request.getNewRdn();
        for (String attributeName : newRdn.getNames()) {
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            Object value = interpreter.eval(fieldRef);
            if (value == null) continue;

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef, value));
        }

        Filter filter = null;

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            String variable = fieldRef.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf(".");
            String sn = variable.substring(0, i);
            String fn = variable.substring(i + 1);

            Attributes fields = sourceValues.get(sn);
            if (fields == null) continue;
            
            Object value = fields.getValue(fn);
            if (value == null) continue;

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
        }

        statement.setFilter(filter);

        interpreter.clear();

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(0, updateRequest);
    }
}
