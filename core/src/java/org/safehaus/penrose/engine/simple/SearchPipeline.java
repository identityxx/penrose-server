package org.safehaus.penrose.engine.simple;

import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Results;
import org.safehaus.penrose.engine.EntryData;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.AttributeMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SearchPipeline extends Pipeline {

    public Logger log = LoggerFactory.getLogger(getClass());

    public SearchPipeline(Results parent) {
        super(parent);
    }

    public void add(Object object) throws Exception {
        boolean debug = log.isDebugEnabled();

        EntryData data = (EntryData)object;

        AttributeValues attributeValues = computeAttributeValues(data);

        if (debug) {
            log.debug("Attribute values:");
            attributeValues.print();
        }

        Entry entry = new Entry(data.getDn(), data.getEntryMapping(), attributeValues, data.getMergedValues());

        super.add(entry);
    }

    public AttributeValues computeAttributeValues(EntryData data) {

        EntryMapping entryMapping = data.getEntryMapping();
        AttributeValues sourceValues = data.getMergedValues();
        AttributeValues attributeValues = new AttributeValues();

        Collection attributeMappings = entryMapping.getAttributeMappings();

        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            String name = attributeMapping.getName();

            String constant = (String)attributeMapping.getConstant();
            if (constant != null) {
                attributeValues.add(name, constant);
                continue;
            }

            String variable = attributeMapping.getVariable();
            if (variable != null) {
                Collection values = sourceValues.get(variable);
                attributeValues.add(name, values);
                continue;
            }
        }

        Collection objectClasses = entryMapping.getObjectClasses();
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            attributeValues.add("objectClass", objectClass);
        }

        return attributeValues;
    }
}
