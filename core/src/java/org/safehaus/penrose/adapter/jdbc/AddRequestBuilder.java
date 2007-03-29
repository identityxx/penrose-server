package org.safehaus.penrose.adapter.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.jdbc.InsertStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AddRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    Collection sources;

    AttributeValues sourceValues;
    Interpreter interpreter;

    AddRequest request;
    AddResponse response;

    Collection requests = new ArrayList();

    public AddRequestBuilder(
            Collection sources,
            AttributeValues sourceValues,
            Interpreter interpreter,
            AddRequest request,
            AddResponse response
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

        InsertStatement statement = new InsertStatement();
        Collection parameters = new ArrayList();

        statement.setSource(sourceRef.getSource());

        interpreter.set(sourceValues);

        Attributes attributes = request.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String attributeName = attribute.getName();
            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(attributeName, attributeValue);
        }

        for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)k.next();
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();
            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addField(field);
            parameters.add(new Parameter(field, value));
        }

        interpreter.clear();

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);
        updateRequest.setParameters(parameters);

        requests.add(updateRequest);
    }

    public void generateSecondaryRequests(
            SourceRef sourceRef
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        Attributes attributes = request.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            for (Iterator j=attributeValues.iterator(); j.hasNext(); ) {
                Object attributeValue = j.next();

                interpreter.set(sourceValues);
                interpreter.set(attributeName, attributeValue);

                Map values = new HashMap();

                for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
                    FieldRef fieldRef = (FieldRef)k.next();
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
            Map values
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Inserting values into "+sourceName);

        InsertStatement statement = new InsertStatement();
        Collection parameters = new ArrayList();

        statement.setSource(sourceRef.getSource());

        for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)k.next();
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            String variable = fieldMapping.getVariable();
            if (variable == null) continue;

            Object value = sourceValues.getOne(variable);
            if (value == null) continue;

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addField(field);
            parameters.add(new Parameter(field, value));
        }

        for (Iterator i=values.keySet().iterator(); i.hasNext(); ) {
            String fieldName = (String)i.next();
            Object value = values.get(fieldName);

            FieldRef fieldRef = sourceRef.getFieldRef(fieldName);
            Field field = fieldRef.getField();

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addField(field);
            parameters.add(new Parameter(field, value));
        }

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);
        updateRequest.setParameters(parameters);

        requests.add(updateRequest);
    }
}
