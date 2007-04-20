package org.safehaus.penrose.source;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.Iterator;
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

    public void add(SearchResult object) throws Exception {

        boolean debug = log.isDebugEnabled();

        SearchResult sourceEntry = (SearchResult)object;

        DN dn = sourceEntry.getDn();
        Attributes attributes = sourceEntry.getAttributes();

        if (debug) {
            log.debug("Synchronizing "+dn);
        }

        SourceValues sourceValues = new SourceValues();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            sourceValues.set(source.getName()+"."+attribute.getName(), attribute.getValues());
        }

        interpreter.set(sourceValues);

        for (Iterator i= destinations.iterator(); i.hasNext(); ) {
            Source destination = (Source)i.next();

            Attributes newAttributes = new Attributes();
            Attributes newRdns = new Attributes();

            for (Iterator j=destination.getFields().iterator(); j.hasNext(); ) {
                Field field = (Field)j.next();
                String fieldName = field.getName();

                Object value = interpreter.eval(field);
                if (value == null) {
                    if (field.isPrimaryKey()) newRdns = null;
                    continue;
                }

                if (value instanceof Collection) {
                    Collection list = (Collection)value;
                    newAttributes.addValues(fieldName, list);
                    if (field.isPrimaryKey()) newRdns.addValues(fieldName, list);
                } else {
                    newAttributes.addValue(fieldName, value);
                    if (field.isPrimaryKey()) newRdns.addValue(fieldName, value);
                }
            }

            if (newRdns == null) continue;
            
            Collection rdns = TransformEngine.convert(newRdns);

            for (Iterator j=rdns.iterator(); j.hasNext(); ) {
                RDN rdn = (RDN)j.next();
                DN newDn = new DN(rdn);

                if (debug) {
                    log.debug("Adding "+destination.getName()+": "+newDn);
                    newAttributes.print();
                }

                destination.add(newDn, newAttributes);
            }
        }

        interpreter.clear();
    }
}
