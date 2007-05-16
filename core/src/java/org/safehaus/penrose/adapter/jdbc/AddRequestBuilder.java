package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.InsertStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.jdbc.Assignment;
import org.safehaus.penrose.jdbc.Request;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.ldap.AddRequest;
import org.safehaus.penrose.ldap.AddResponse;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddRequestBuilder extends RequestBuilder {

    Collection<SourceRef> sourceRefs;

    SourceValues sourceValues;
    Interpreter interpreter;

    AddRequest request;
    AddResponse response;

    public AddRequestBuilder(
            Collection<SourceRef> sourceRefs,
            SourceValues sourceValues,
            Interpreter interpreter,
            AddRequest request,
            AddResponse response
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

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        InsertStatement statement = new InsertStatement();

        statement.setSource(sourceRef.getSource());

        interpreter.set(sourceValues);

        Attributes attributes = request.getAttributes();
        for (Attribute attribute : attributes.getAll()) {

            String attributeName = attribute.getName();
            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(attributeName, attributeValue);
        }

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();
            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef, value));
        }

        interpreter.clear();

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

        Attributes attributes = request.getAttributes();
        for (Attribute attribute : attributes.getAll()) {

            String attributeName = attribute.getName();
            Collection<Object> attributeValues = attribute.getValues();

            for (Object attributeValue : attributeValues) {
                interpreter.set(sourceValues);
                interpreter.set(attributeName, attributeValue);

                Map<String,Object> values = new HashMap<String,Object>();

                for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
                    FieldMapping fieldMapping = fieldRef.getFieldMapping();

                    String variable = fieldMapping.getVariable();
                    if (variable != null) {
                        if (variable.indexOf(".") >= 0) continue; // skip foreign key
                    }

                    String fieldName = fieldRef.getName();
                    Object value = interpreter.eval(fieldMapping);
                    if (value == null) continue;

                    values.put(fieldName, value);
                }

                if (!values.isEmpty()) {
                    generateInsertStatement(
                            sourceRef,
                            values
                    );
                }

                interpreter.clear();
            }

        }
    }

    public void generateInsertStatement(
            SourceRef sourceRef,
            Map<String,Object> values
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Inserting values into "+sourceName);

        InsertStatement statement = new InsertStatement();

        statement.setSource(sourceRef.getSource());

        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            Object value = sourceValues.getOne(variable);
            if (value == null) continue;

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef, value));
        }

        for (String fieldName : values.keySet()) {
            Object value = values.get(fieldName);

            FieldRef fieldRef = sourceRef.getFieldRef(fieldName);
            Field field = fieldRef.getField();

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef, value));
        }

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(updateRequest);
    }
}
