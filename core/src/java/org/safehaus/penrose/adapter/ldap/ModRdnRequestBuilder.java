package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.ModRdnRequest;
import org.safehaus.penrose.session.ModRdnResponse;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModRdnRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    LDAPAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    ModRdnRequest request;
    ModRdnResponse response;

    Collection requests = new ArrayList();

    public ModRdnRequestBuilder(
            LDAPAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        this.adapter = adapter;

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sourceMappings = sourceMappings;
        primarySourceMapping = (SourceMapping)sourceMappings.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        PenroseContext penroseContext = adapter.getPenroseContext();
        interpreter = penroseContext.getInterpreterManager().newInstance();
    }

    public Collection generate() throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        generatePrimaryRequest(sourceMapping);

        return requests;
    }

    public void generatePrimaryRequest(SourceMapping sourceMapping) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        ModRdnRequest newRequest = new ModRdnRequest();

        interpreter.set(sourceValues);

        RDN rdn = request.getDn().getRdn();
        for (Iterator i=rdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = rdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        RDNBuilder rb = new RDNBuilder();

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            String fieldName = fieldMapping.getName();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
            if (!fieldConfig.isPrimaryKey()) continue;

            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            rb.set(fieldConfig.getOriginalName(), value);
        }

        newRequest.setDn(adapter.getDn(sourceConfig, rb.toRdn()));

        interpreter.clear();
        interpreter.set(sourceValues);

        RDN newRdn = request.getNewRdn();
        for (Iterator i=newRdn.getNames().iterator(); i.hasNext(); ) {
            String attributeName = (String)i.next();
            Object attributeValue = newRdn.get(attributeName);

            interpreter.set(attributeName, attributeValue);
        }

        rb.clear();

        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            String fieldName = fieldMapping.getName();
            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
            if (!fieldConfig.isPrimaryKey()) continue;

            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            if (debug) log.debug(" - Field: "+fieldName+": "+value);
            rb.set(fieldConfig.getOriginalName(), value);
        }

        newRequest.setNewRdn(rb.toRdn());

        requests.add(newRequest);
    }
}
