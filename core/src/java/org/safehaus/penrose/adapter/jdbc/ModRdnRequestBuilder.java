package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.ModRdnRequest;
import org.safehaus.penrose.ldap.ModRdnResponse;
import org.safehaus.penrose.jdbc.UpdateStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.jdbc.Assignment;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.Source;
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

    AttributeValues sourceValues;
    Interpreter interpreter;

    ModRdnRequest request;
    ModRdnResponse response;

    AttributeValues newSourceValues = new AttributeValues();

    public ModRdnRequestBuilder(
            Collection sources,
            AttributeValues sourceValues,
            Interpreter interpreter,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        this.sources = sources;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection generate() throws Exception {

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

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        UpdateStatement statement = new UpdateStatement();

        Source source = sourceRef.getSource();
        statement.setSource(source);

        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        newSourceValues.set(sourceValues);

        for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)k.next();
            Field field = fieldRef.getField();
            String fieldName = field.getName();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            statement.addAssignment(new Assignment(fieldRef, value));

            newSourceValues.set(sourceName+"."+fieldName, value);
        }

        Filter filter = null;

        for (Iterator i=sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = sourceValues.getOne(name);

            int p = name.indexOf(".");
            String sn = name.substring(0, p);
            String fn = name.substring(p+1);

            if (!sourceName.equals(sn)) continue;

            FieldRef fieldRef = sourceRef.getFieldRef(fn);

            SimpleFilter sf = new SimpleFilter(fn, "=", value);
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

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        UpdateStatement statement = new UpdateStatement();

        Source source = sourceRef.getSource();
        statement.setSource(source);

        interpreter.set(newSourceValues);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = newRdn.get(attributeName);

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
            statement.addAssignment(new Assignment(fieldRef, value));
        }

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

            SimpleFilter sf = new SimpleFilter(fieldName, "=", value);
            filter = FilterTool.appendAndFilter(filter, sf);

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
        }

        statement.setFilter(filter);

        interpreter.clear();

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);

        requests.add(0, updateRequest);
    }
}
