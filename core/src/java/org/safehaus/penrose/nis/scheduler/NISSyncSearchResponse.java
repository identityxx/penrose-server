package org.safehaus.penrose.nis.scheduler;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.TransformationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class NISSyncSearchResponse extends SearchResponse {

    public Logger log = LoggerFactory.getLogger(getClass());
    public Logger errorLog = org.safehaus.penrose.log.Error.log;
    public boolean debug = log.isDebugEnabled();

    protected Source source;
    protected Collection<Source> destinations;

    protected Interpreter interpreter;

    public NISSyncSearchResponse(
            Source source,
            Collection<Source> snapshots,
            Interpreter interpreter
    ) throws Exception {

        this.source = source;
        this.destinations = snapshots;
        this.interpreter = interpreter;

    }

    public void add(SearchResult result) throws Exception {

        DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        if (debug) {
            log.debug("Loading "+dn);
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

            Collection<RDN> rdns = TransformationUtil.convert(newRdns);

            for (RDN rdn : rdns) {
                DN newDn = new DN(rdn);

                if (debug) {
                    log.debug("Adding " + destination.getName() + ": " + newDn);
                    newAttributes.print();
                }

                try {
                    destination.add(null, newDn, newAttributes);
                } catch (Exception e) {
                    errorLog.error(e.getMessage(), e);
                }
            }
        }

        interpreter.clear();
    }
}
