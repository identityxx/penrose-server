package org.safehaus.penrose.source;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SourceSyncSearchResponse extends SearchResponse<SearchResult> {

    protected Source source;
    protected Collection<Source> destinations;

    protected Interpreter interpreter;

    public SourceSyncSearchResponse(
            Source source,
            Collection<Source> snapshots,
            Interpreter interpreter
    ) throws Exception {

        this.source = source;
        this.destinations = snapshots;
        this.interpreter = interpreter;

    }

    public void add(SearchResult result) throws Exception {

        boolean debug = log.isDebugEnabled();

        DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        if (debug) {
            log.debug("Synchronizing "+dn);
        }

        SourceValues sourceValues = new SourceValues();
        Attributes attrs = sourceValues.get(source.getName());
        for (Attribute attribute : attributes.getAll()) {
            attrs.setValues(attribute.getName(), attribute.getValues());
        }

        interpreter.set(sourceValues);

        for (Source destination : destinations) {

            Attributes newAttributes = new Attributes();
            Attributes newRdns = new Attributes();

            for (Field field : destination.getFields()) {
                String fieldName = field.getName();

                Object value = interpreter.eval(field);
                if (value == null) {
                    if (field.isPrimaryKey()) newRdns = null;
                    continue;
                }

                if (value instanceof Collection) {
                    Collection<Object> list = (Collection<Object>) value;
                    newAttributes.addValues(fieldName, list);
                    if (field.isPrimaryKey()) newRdns.addValues(fieldName, list);
                } else {
                    newAttributes.addValue(fieldName, value);
                    if (field.isPrimaryKey()) newRdns.addValue(fieldName, value);
                }
            }

            if (newRdns == null) continue;

            Collection<RDN> rdns = TransformEngine.convert(newRdns);

            for (RDN rdn : rdns) {
                DN newDn = new DN(rdn);

                if (debug) {
                    log.debug("Adding " + destination.getName() + ": " + newDn);
                    newAttributes.print();
                }

                destination.add(newDn, newAttributes);
            }
        }

        interpreter.clear();
    }
}
