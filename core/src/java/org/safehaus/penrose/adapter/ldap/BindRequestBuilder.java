package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.BindRequest;
import org.safehaus.penrose.ldap.BindResponse;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class BindRequestBuilder extends RequestBuilder {

    String suffix;

    Collection sourceRefs;
    AttributeValues sourceValues;

    Interpreter interpreter;

    BindRequest request;
    BindResponse response;

    public BindRequestBuilder(
            String suffix,
            Collection sources,
            AttributeValues sourceValues,
            Interpreter interpreter,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        this.suffix = suffix;

        this.sourceRefs = sources;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public BindRequest generate() throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        generatePrimaryRequest(sourceRef);

        return (BindRequest)requests.iterator().next();
    }

    public void generatePrimaryRequest(SourceRef sourceRef) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        BindRequest newRequest = new BindRequest();

        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        RDNBuilder rb = new RDNBuilder();

        for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)k.next();
            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            String fieldName = fieldRef.getName();
            if (!fieldRef.isPrimaryKey()) continue;

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            rb.set(fieldRef.getOriginalName(), value);
        }

        Source source = sourceRef.getSource();
        newRequest.setDn(getDn(source, rb.toRdn()));
        newRequest.setPassword(request.getPassword());

        requests.add(newRequest);
    }
}
