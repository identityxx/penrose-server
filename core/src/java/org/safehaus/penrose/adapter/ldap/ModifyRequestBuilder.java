package org.safehaus.penrose.adapter.ldap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.session.ModifyRequest;
import org.safehaus.penrose.session.ModifyResponse;
import org.safehaus.penrose.session.Modification;
import org.safehaus.penrose.naming.PenroseContext;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModifyRequestBuilder {
    
    Logger log = LoggerFactory.getLogger(getClass());

    LDAPAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    ModifyRequest request;
    ModifyResponse response;

    Collection requests = new ArrayList();

    public ModifyRequestBuilder(
            LDAPAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
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

        ModifyRequest newRequest = new ModifyRequest();

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

        Collection newModifications = new ArrayList();

        Collection modifications = request.getModifications();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            String attributeName = attribute.getName();
            Collection attributeValues = attribute.getValues();

            if (debug) {
                switch (type) {
                    case Modification.ADD:
                        log.debug("Adding attribute "+attributeName+": "+attributeValues);
                        break;
                    case Modification.REPLACE:
                        log.debug("Replacing attribute "+attributeName+": "+attributeValues);
                        break;
                    case Modification.DELETE:
                        log.debug("Deleting attribute "+attributeName+": "+attributeValues);
                        break;
                }
            }

            interpreter.set(sourceValues);
            interpreter.set(attributeName, attributeValues);

            switch (type) {
                case Modification.ADD:
                case Modification.REPLACE:
                    for (Iterator j=fieldMappings.iterator(); j.hasNext(); ) {
                        FieldMapping fieldMapping = (FieldMapping)j.next();
                        String fieldName = fieldMapping.getName();
                        FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                        if (fieldConfig.isPrimaryKey()) continue;

                        Object value = interpreter.eval(entryMapping, fieldMapping);
                        if (value == null) continue;

                        if (debug) log.debug("Setting field "+fieldName+" to "+value);

                        Attribute newAttribute = new Attribute(fieldConfig.getOriginalName());
                        if (value instanceof Collection) {
                            for (Iterator k=((Collection)value).iterator(); k.hasNext(); ) {
                                Object v = k.next();
                                newAttribute.addValue(v);
                            }
                        } else {
                            newAttribute.addValue(value);
                        }
                        newModifications.add(new Modification(type, newAttribute));
                    }
                    break;

                case Modification.DELETE:
                    for (Iterator j=fieldMappings.iterator(); j.hasNext(); ) {
                        FieldMapping fieldMapping = (FieldMapping)j.next();
                        String fieldName = fieldMapping.getName();
                        FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

                        String variable = fieldMapping.getVariable();
                        if (variable == null) {
                            Object value = interpreter.eval(entryMapping, fieldMapping);
                            if (value == null) continue;

                            if (debug) log.debug("Setting field "+fieldName+" to null");

                            Attribute newAttribute = new Attribute(fieldConfig.getOriginalName());
                            newAttribute.addValue(value);
                            newModifications.add(new Modification(type, newAttribute));

                        } else {
                            if (!variable.equals(attributeName)) continue;

                            Attribute newAttribute = new Attribute(fieldConfig.getOriginalName());
                            for (Iterator k=attributeValues.iterator(); k.hasNext(); ) {
                                Object value = k.next();
                                newAttribute.addValue(value);
                            }
                            newModifications.add(new Modification(type, newAttribute));
                        }

                    }
                    break;
            }

            interpreter.clear();
        }

        newRequest.setModifications(newModifications);

        requests.add(newRequest);
    }
}
