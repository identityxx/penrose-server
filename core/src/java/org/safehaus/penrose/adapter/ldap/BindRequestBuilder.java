package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.BindRequest;
import org.safehaus.penrose.session.BindResponse;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class BindRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    LDAPAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    BindRequest request;
    BindResponse response;

    Collection requests = new ArrayList();

    public BindRequestBuilder(
            LDAPAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            BindRequest request,
            BindResponse response
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

    public BindRequest generate() throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        generatePrimaryRequest(sourceMapping);

        return (BindRequest)requests.iterator().next();
    }

    public void generatePrimaryRequest(SourceMapping sourceMapping) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        BindRequest newRequest = new BindRequest();

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
        newRequest.setPassword(request.getPassword());

        requests.add(newRequest);
    }
}
