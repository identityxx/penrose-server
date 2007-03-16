package org.safehaus.penrose.engine.basic;

import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.engine.EntryData;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.EntryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class BasicEngineSearchResponse extends SearchResponse {

    public Logger log = LoggerFactory.getLogger(getClass());

    Interpreter interpreter;

    SearchResponse parent;

    public BasicEngineSearchResponse(SearchResponse parent, Interpreter interpreter) {
        this.parent = parent;
        this.interpreter = interpreter;
    }

    public void add(Object object) throws Exception {
        boolean debug = log.isDebugEnabled();

        EntryData data = (EntryData)object;

        AttributeValues attributeValues = computeAttributeValues(data);

        if (debug) {
            log.debug("Attribute values:");
            attributeValues.print();
        }

        Attributes attributes = EntryUtil.computeAttributes(attributeValues);
        Entry entry = new Entry(data.getDn(), data.getEntryMapping(), attributes, data.getMergedValues());

        parent.add(entry);
    }

    public AttributeValues computeAttributeValues(EntryData data) throws Exception {

        EntryMapping entryMapping = data.getEntryMapping();
        AttributeValues sourceValues = data.getMergedValues();
        interpreter.set(sourceValues);

        AttributeValues attributeValues = new AttributeValues();

        Collection attributeMappings = entryMapping.getAttributeMappings();

        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

            Object value = interpreter.eval(entryMapping, attributeMapping);
            if (value == null) continue;

            attributeValues.add(attributeMapping.getName(), value);
        }

        Collection objectClasses = entryMapping.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributeValues.add("objectClass", objectClass);
        }

        return attributeValues;
    }
}
