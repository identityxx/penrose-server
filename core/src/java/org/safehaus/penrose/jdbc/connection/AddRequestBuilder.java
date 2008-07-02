package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.directory.FieldRef;
import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.Assignment;
import org.safehaus.penrose.jdbc.InsertStatement;
import org.safehaus.penrose.jdbc.Statement;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.Field;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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

    public Collection<Statement> generate() throws Exception {

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

        InsertStatement statement = new InsertStatement();

        statement.setSourceName(sourceRef.getSource().getName());

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

            Object value = interpreter.eval(fieldRef);
            if (value == null) continue;

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef.getOriginalName(), value));
        }

        interpreter.clear();

        requests.add(statement);
    }

    public void generateSecondaryRequests(
            SourceRef sourceRef
    ) throws Exception {

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

                    String variable = fieldRef.getVariable();
                    if (variable != null) {
                        if (variable.indexOf(".") >= 0) continue; // skip foreign key
                    }

                    String fieldName = fieldRef.getName();
                    Object value = interpreter.eval(fieldRef);
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

        String sourceName = sourceRef.getAlias();

        if (debug) log.debug("Inserting values into "+sourceName);

        InsertStatement statement = new InsertStatement();

        statement.setSourceName(sourceRef.getSource().getName());

        if (debug) log.debug("Fields:");
        for (FieldRef fieldRef : sourceRef.getFieldRefs()) {
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            String variable = fieldRef.getVariable();
            if (variable == null) continue;

            if (debug) log.debug(" - " + fieldName + ": " + variable);

            int i = variable.indexOf(".");
            if (i < 0) continue;
            
            String sn = variable.substring(0, i);
            String fn = variable.substring(i + 1);

            Attributes fields = sourceValues.get(sn);
            if (fields == null) continue;

            Object value = fields.getValue(fn);
            if (value == null) continue;

            if (debug) log.debug("   - value: " + value);
            statement.addAssignment(new Assignment(fieldRef.getOriginalName(), value));
        }

        if (debug) log.debug("Fields:");
        for (String fieldName : values.keySet()) {
            Object value = values.get(fieldName);
            if (debug) log.debug(" - " + fieldName + ": " + value);

            FieldRef fieldRef = sourceRef.getFieldRef(fieldName);

            statement.addAssignment(new Assignment(fieldRef.getOriginalName(), value));
        }

        requests.add(statement);
    }
}
