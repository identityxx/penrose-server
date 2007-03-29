package org.safehaus.penrose.adapter.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.ModRdnRequest;
import org.safehaus.penrose.session.ModRdnResponse;
import org.safehaus.penrose.jdbc.UpdateStatement;
import org.safehaus.penrose.jdbc.UpdateRequest;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.Filter;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class ModRdnRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    Collection sources;

    AttributeValues sourceValues;
    Interpreter interpreter;

    ModRdnRequest request;
    ModRdnResponse response;

    AttributeValues newSourceValues = new AttributeValues();

    List requests = new ArrayList();

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
        Collection parameters = new ArrayList();

        statement.setSource(sourceRef);

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
            statement.addField(fieldRef);
            parameters.add(new Parameter(field, value));

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
            Field field = fieldRef.getField();

            SimpleFilter sf = new SimpleFilter(fn, "=", "?");
            filter = FilterTool.appendAndFilter(filter, sf);

            parameters.add(new Parameter(field, value));
        }

        statement.setFilter(filter);

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

        UpdateStatement statement = new UpdateStatement();
        Collection parameters = new ArrayList();

        statement.setSource(sourceRef);

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
            statement.addField(fieldRef);
            parameters.add(new Parameter(field, value));
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

            SimpleFilter sf = new SimpleFilter(fieldName, "=", "?");
            filter = FilterTool.appendAndFilter(filter, sf);

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            parameters.add(new Parameter(field, value));
        }

        statement.setFilter(filter);

        interpreter.clear();

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.setStatement(statement);
        updateRequest.setParameters(parameters);

        requests.add(0, updateRequest);
    }
}
