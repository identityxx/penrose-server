package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class ModifyRequestBuilder extends RequestBuilder {
    
    String suffix;

    Collection sourceRefs;
    SourceValues sourceValues;

    Interpreter interpreter;

    ModifyRequest request;
    ModifyResponse response;

    public ModifyRequestBuilder(
            String suffix,
            Collection sources,
            SourceValues sourceValues,
            Interpreter interpreter,
            ModifyRequest request,
            ModifyResponse response
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

        ModifyRequest newRequest = new ModifyRequest();

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

        Collection<Modification> newModifications = new ArrayList<Modification>();

        Collection<Modification> modifications = request.getModifications();
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
                    for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                        FieldRef fieldRef = (FieldRef)j.next();
                        FieldMapping fieldMapping = fieldRef.getFieldMapping();
                        String fieldName = fieldRef.getName();
                        if (fieldRef.isPrimaryKey()) continue;

                        Object value = interpreter.eval(fieldMapping);
                        if (value == null) continue;

                        if (debug) log.debug("Setting field "+fieldName+" to "+value);

                        Attribute newAttribute = new Attribute(fieldRef.getOriginalName());
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
                    for (Iterator j= sourceRef.getFieldRefs().iterator(); j.hasNext(); ) {
                        FieldRef fieldRef = (FieldRef)j.next();
                        FieldMapping fieldMapping = fieldRef.getFieldMapping();
                        
                        String fieldName = fieldRef.getName();

                        String variable = fieldMapping.getVariable();
                        if (variable == null) {
                            Object value = interpreter.eval(fieldMapping);
                            if (value == null) continue;

                            if (debug) log.debug("Setting field "+fieldName+" to null");

                            Attribute newAttribute = new Attribute(fieldRef.getOriginalName());
                            newAttribute.addValue(value);
                            newModifications.add(new Modification(type, newAttribute));

                        } else {
                            if (!variable.equals(attributeName)) continue;

                            Attribute newAttribute = new Attribute(fieldRef.getOriginalName());
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
