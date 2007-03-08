package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Relationship;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.partition.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class EngineTool {

    public static Logger log = LoggerFactory.getLogger(EngineTool.class);

    public static void propagateUp(
            Partition partition,
            EntryMapping entryMapping,
            AttributeValues sourceValues
    ) {

        boolean debug = log.isDebugEnabled();

        EntryMapping em = entryMapping;
        while (em != null) {
            //log.debug("Mapping: "+em.getDn());

            Collection relationships = em.getRelationships();
            for (Iterator i=relationships.iterator(); i.hasNext(); ) {
                Relationship relationship = (Relationship)i.next();

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

            em = partition.getParent(em);
        }
    }
}
