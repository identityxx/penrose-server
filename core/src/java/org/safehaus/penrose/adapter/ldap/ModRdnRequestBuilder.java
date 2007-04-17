package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.ModRdnRequest;
import org.safehaus.penrose.ldap.ModRdnResponse;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.ldap.RDNBuilder;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModRdnRequestBuilder extends RequestBuilder {

    String suffix;

    Collection sourceRefs;
    SourceValues sourceValues;

    Interpreter interpreter;

    ModRdnRequest request;
    ModRdnResponse response;

    public ModRdnRequestBuilder(
            String suffix,
            Collection sources,
            SourceValues sourceValues,
            Interpreter interpreter,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        this.suffix = suffix;

        this.sourceRefs = sources;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection generate() throws Exception {

        SourceRef sourceRef = (SourceRef) sourceRefs.iterator().next();
        generatePrimaryRequest(sourceRef);

        return requests;
    }

    public void generatePrimaryRequest(SourceRef sourceRef) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();
        if (debug) log.debug("Processing source "+sourceName);

        ModRdnRequest newRequest = new ModRdnRequest();

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

        interpreter.clear();
        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        rb.clear();

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

        newRequest.setNewRdn(rb.toRdn());

        requests.add(newRequest);
    }
}
