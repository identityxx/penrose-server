package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Relationship;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class EngineTool {

    public static Logger log = LoggerFactory.getLogger(EngineTool.class);

    public static void propagateUp(
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues sourceValues
    ) throws Exception {

        List mappings = new ArrayList();

        while (entryMapping != null) {
            mappings.add(entryMapping);
            entryMapping = partition.getParent(entryMapping);
        }

        propagate(mappings, sourceValues);
    }

    public static void propagate(Collection mappings, AttributeValues sourceValues) throws Exception {

        boolean debug = log.isDebugEnabled();

        for (Iterator i=mappings.iterator(); i.hasNext(); ) {
            EntryMapping em = (EntryMapping)i.next();

            Collection relationships = em.getRelationships();
            for (Iterator j=relationships.iterator(); j.hasNext(); ) {
                Relationship relationship = (Relationship)j.next();

                String lhs = relationship.getLhs();
                String rhs = relationship.getRhs();

                Collection values = sourceValues.get(lhs);
                if (values == null) {
                    values = sourceValues.get(rhs);
                    if (values != null) {
                        sourceValues.set(lhs, values);
                        if (debug) log.debug("Propagating "+lhs+": "+values);
                    }
                } else {
                    sourceValues.set(rhs, values);
                    if (debug) log.debug("Propagating "+rhs+": "+values);
                }
            }
        }
    }
}
