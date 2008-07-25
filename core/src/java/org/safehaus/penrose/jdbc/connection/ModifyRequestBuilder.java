package org.safehaus.penrose.jdbc.connection;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.SourceAttributes;
import org.safehaus.penrose.directory.EntrySource;
import org.safehaus.penrose.directory.EntryField;
import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.jdbc.*;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ModifyRequestBuilder extends RequestBuilder {

    Collection<EntrySource> sourceRefs;

    SourceAttributes sourceValues;
    Interpreter interpreter;

    ModifyRequest request;
    ModifyResponse response;

    public ModifyRequestBuilder(
            Collection<EntrySource> sourceRefs,
            SourceAttributes sourceValues,
            Interpreter interpreter,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        this.sourceRefs = sourceRefs;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection<Statement> generate() throws Exception {

        boolean first = true;
        for (EntrySource sourceRef : sourceRefs) {

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
            EntrySource sourceRef
    ) throws Exception {

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        UpdateStatement statement = new UpdateStatement();

        statement.setSource(sourceRef.getSource().getPartition().getName(), sourceRef.getSource().getName());

        Collection<Modification> modifications = request.getModifications();
        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            if (debug) {
                switch (type) {
                    case Modification.ADD:
                        log.debug("Adding attribute " + attributeName + ": " + attributeValues);
                        break;
                    case Modification.REPLACE:
                        log.debug("Replacing attribute " + attributeName + ": " + attributeValues);
                        break;
                    case Modification.DELETE:
                        log.debug("Deleting attribute " + attributeName + ": " + attributeValues);
                        break;
                }
            }

            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(sourceValues);
            interpreter.set(attributeName, attributeValue);

            switch (type) {
                case Modification.ADD:
                case Modification.REPLACE:
                    for (EntryField fieldRef : sourceRef.getFields()) {
                        Field field = fieldRef.getField();

                        Object value = interpreter.eval(fieldRef);
                        if (value == null) continue;

                        String fieldName = field.getName();
                        if (debug) log.debug("Setting field " + fieldName + " to " + value);

                        statement.addAssignment(new Assignment(fieldRef.getOriginalName(), value));
                    }
                    break;

                case Modification.DELETE:
                    for (EntryField fieldRef : sourceRef.getFields()) {
                        Field field = fieldRef.getField();

                        String variable = fieldRef.getVariable();
                        if (variable == null) continue;

                        if (!variable.equals(attributeName)) continue;

                        String fieldName = field.getName();
                        if (debug) log.debug("Setting field " + fieldName + " to null");

                        statement.addAssignment(new Assignment(fieldRef.getOriginalName(), null));
                    }
                    break;
            }

            interpreter.clear();
        }

        if (statement.isEmpty()) return;

        Filter filter = null;

        Attributes attributes = sourceValues.get(sourceName);

        for (String fieldName : attributes.getNames()) {

            Object value = attributes.getValue(fieldName);

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);
        }

        statement.setFilter(filter);

        requests.add(statement);
    }

    public void generateSecondaryRequests(
            EntrySource sourceRef
    ) throws Exception {

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        Collection<Modification> modifications = request.getModifications();
        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            if (debug) {
                switch (type) {
                    case Modification.ADD:
                        log.debug("Adding attribute " + attributeName + ": " + attributeValues);
                        break;
                    case Modification.REPLACE:
                        log.debug("Replacing attribute " + attributeName + ": " + attributeValues);
                        break;
                    case Modification.DELETE:
                        log.debug("Deleting attribute " + attributeName + ": " + attributeValues);
                        break;
                }
            }

            if (attributeValues.isEmpty()) {
                for (EntryField fieldRef : sourceRef.getFields()) {

                    String variable = fieldRef.getVariable();
                    if (variable != null) {
                        if (variable.indexOf(".") >= 0) continue; // skip foreign key
                    }

                    if (attributeName.equals(variable)) {
                        generateDeleteStatement(
                                sourceRef
                        );
                        continue;
                    }

                    Expression expression = fieldRef.getExpression();
                    if (expression == null) continue;

                    String foreach = expression.getForeach();
                    if (foreach == null) continue;

                    if (attributeName.equals(foreach)) {
                        generateDeleteStatement(
                                sourceRef
                        );
                        continue;
                    }

                }
                continue;
            }

            boolean first = true;

            for (Object attributeValue : attributeValues) {
                interpreter.set(sourceValues);
                interpreter.set(attributeName, attributeValue);

                Map<String,Object> values = new HashMap<String,Object>();

                for (EntryField fieldRef : sourceRef.getFields()) {

                    String variable = fieldRef.getVariable();
                    if (variable != null) {
                        if (variable.indexOf(".") >= 0) continue; // skip foreign key
                    }

                    String fieldName = fieldRef.getName();
                    Object value = interpreter.eval(fieldRef);
                    if (value == null) continue;

                    values.put(fieldName, value);
                }

                if (values.isEmpty()) continue;

                switch (type) {
                    case Modification.ADD:
                        generateInsertStatement(
                                sourceRef,
                                values
                        );
                        break;

                    case Modification.REPLACE:
                        if (first) {
                            generateDeleteStatement(
                                    sourceRef
                            );
                            first = false;
                        }
                        generateInsertStatement(
                                sourceRef,
                                values
                        );
                        break;

                    case Modification.DELETE:
                        generateDeleteStatement(
                                sourceRef,
                                values
                        );
                        break;
                }

                interpreter.clear();
            }

        }
    }

    public void generateInsertStatement(
            EntrySource sourceRef,
            Map<String,Object> values
    ) throws Exception {

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Inserting values into "+sourceName);

        InsertStatement statement = new InsertStatement();

        statement.setSource(sourceRef.getSource().getPartition().getName(), sourceRef.getSource().getName());

        for (EntryField fieldRef : sourceRef.getFields()) {
            Field field = fieldRef.getField();

            String variable = fieldRef.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf(".");
            String sn = variable.substring(0, i);
            String fn = variable.substring(i + 1);

            Attributes fields = sourceValues.get(sn);
            if (fields == null) continue;

            Object value = fields.getValue(fn);
            if (value == null) continue;

            String fieldName = field.getName();

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef.getOriginalName(), value));
        }

        for (String fieldName : values.keySet()) {
            Object value = values.get(fieldName);

            EntryField fieldRef = sourceRef.getField(fieldName);

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            statement.addAssignment(new Assignment(fieldRef.getOriginalName(), value));
        }

        requests.add(statement);
    }

    public void generateDeleteStatement(
            EntrySource sourceRef
    ) throws Exception {
        generateDeleteStatement(sourceRef, null);
    }

    public void generateDeleteStatement(
            EntrySource sourceRef,
            Map<String,Object> values
    ) throws Exception {

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Deleting rows from "+sourceName);

        DeleteStatement statement = new DeleteStatement();

        statement.setSource(sourceRef.getSource().getPartition().getName(), sourceRef.getSource().getName());

        Filter filter = null;

        for (EntryField fieldRef : sourceRef.getFields()) {
            Field field = fieldRef.getField();

            String variable = fieldRef.getVariable();
            if (variable == null) continue;

            int i = variable.indexOf(".");
            String sn = variable.substring(0, i);
            String fn = variable.substring(i + 1);

            Attributes fields = sourceValues.get(sn);
            if (fields == null) continue;

            Object value = fields.getValue(fn);
            if (value == null) continue;

            String fieldName = field.getName();

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);

            if (debug) log.debug(" - Field: " + fieldName + ": " + value);
        }

        if (values != null) {
            for (String fieldName : values.keySet()) {
                Object value = values.get(fieldName);

                SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
                filter = FilterTool.appendAndFilter(filter, sf);

                if (debug) log.debug(" - Field: " + fieldName + ": " + value);
            }
        }

        statement.setFilter(filter);

        requests.add(statement);
    }
}
