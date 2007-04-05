package org.safehaus.penrose.cache;

import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.interpreter.Interpreter;

import java.util.Iterator;
import java.util.Collection;
import java.util.Map;

/**
 * @author Endi S. Dewata
 */
public class CacheSearchResponse extends SearchResponse {

    protected Source source;
    protected Map<String,Source> caches;
    protected Map<String,Source> snapshots;

    protected Interpreter interpreter;

    public CacheSearchResponse(
            Source source,
            Map<String,Source> caches,
            Map<String,Source> snapshots,
            Interpreter interpreter
    ) throws Exception {

        this.source = source;
        this.caches = caches;
        this.snapshots = snapshots;

        this.interpreter = interpreter;
    }

    public void add(Object object) throws Exception {

        boolean debug = log.isDebugEnabled();

        Entry sourceEntry = (Entry)object;

        DN dn = sourceEntry.getDn();
        Attributes attributes = sourceEntry.getAttributes();

        if (debug) {
            log.debug("Synchronizing "+dn);
        }

        AttributeValues sourceValues = new AttributeValues();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();
            sourceValues.set(source.getName()+"."+attribute.getName(), attribute.getValues());
        }

        interpreter.set(sourceValues);

        for (Iterator i=caches.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Source snapshot = (Source)snapshots.get(name);

            Attributes newAttributes = new Attributes();
            Attributes newRdns = new Attributes();

            for (Iterator j=snapshot.getFields().iterator(); j.hasNext(); ) {
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
                    log.debug("Adding "+snapshot.getName()+": "+newDn);
                    newAttributes.print();
                }

                snapshot.add(newDn, newAttributes);
            }
        }

        interpreter.clear();
    }
}
