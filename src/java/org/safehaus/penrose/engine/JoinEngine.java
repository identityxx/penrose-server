/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.config.Config;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JoinEngine {

    Logger log = Logger.getLogger(getClass());

    Engine engine;

    public JoinEngine(Engine engine) {
        this.engine = engine;
    }

    public Collection join(Collection list1, Collection list2, EntryDefinition entryDefinition, Collection relationships) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=list1.iterator(); i.hasNext(); ) {
            AttributeValues av1 = (AttributeValues)i.next();

            for (Iterator j=list2.iterator(); j.hasNext(); ) {
                AttributeValues av2 = (AttributeValues)j.next();

                if (!evaluate(entryDefinition, relationships, av1, av2)) continue;

                AttributeValues sv = new AttributeValues();
                sv.add(av1);
                sv.add(av2);

                results.add(sv);
/*
                log.debug("Join:");
                log.debug(" - "+av1);
                log.debug(" - "+av2);
*/
            }

        }

        return results;
    }

    public Collection leftJoin(Collection list1, Collection list2, EntryDefinition entryDefinition, Collection relationships) throws Exception {
        Collection results = new ArrayList();

        //log.debug("Left join:");
        for (Iterator i=list1.iterator(); i.hasNext(); ) {
            AttributeValues av1 = (AttributeValues)i.next();
            //log.debug(" - "+av1);

            boolean found = false;

            for (Iterator j=list2.iterator(); j.hasNext(); ) {
                AttributeValues av2 = (AttributeValues)j.next();
                //log.debug("    - "+av2);

                if (evaluate(entryDefinition, relationships, av1, av2)) {

                    AttributeValues sv = new AttributeValues();
                    sv.add(av1);
                    sv.add(av2);

                    results.add(sv);
                    found = true;
                    //log.debug("     => true");
                } else {
                    //log.debug("     => false");
                }

            }

            if (!found) {
                //log.debug("   => no match found");
                results.add(av1);
            }
        }

        return results;
    }

    public boolean evaluate(
            EntryDefinition entryDefinition,
            Collection relationships,
            AttributeValues sv1,
            AttributeValues sv2)
            throws Exception {

        if (relationships == null) return true;

        Config config = engine.getConfig(entryDefinition.getDn());

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();

            String operator = relationship.getOperator();

            String lhs = relationship.getLhs();
            int lindex = lhs.indexOf(".");
            String lsourceName = lhs.substring(0, lindex);
            String lfieldName = lhs.substring(lindex+1);
            Source lsource = config.getEffectiveSource(entryDefinition, lsourceName);
            ConnectionConfig lconnectionConfig = config.getConnectionConfig(lsource.getConnectionName());
            SourceDefinition lsourceDefinition = lconnectionConfig.getSourceDefinition(lsource.getSourceName());
            FieldDefinition lfieldDefinition = lsourceDefinition.getFieldDefinition(lfieldName);

            String rhs = relationship.getRhs();
            int rindex = rhs.indexOf(".");
            String rsourceName = rhs.substring(0, rindex);
            String rfieldName = rhs.substring(rindex+1);
            Source rsource = config.getEffectiveSource(entryDefinition, rsourceName);
            ConnectionConfig rconnectionConfig = config.getConnectionConfig(rsource.getConnectionName());
            SourceDefinition rsourceDefinition = rconnectionConfig.getSourceDefinition(rsource.getSourceName());
            FieldDefinition rfieldDefinition = rsourceDefinition.getFieldDefinition(rfieldName);

            Collection values1 = sv1.get(lhs);
            Collection values2 = sv2.get(rhs);

            if (values1 == null && values2 == null) {
                values1 = sv1.get(rhs);
                values2 = sv2.get(lhs);
            }

            if (values1 == null || values2 == null) return false;

            boolean result = false;

            for (Iterator j=values1.iterator(); !result && j.hasNext(); ) {
                Object object1 = convert(j.next(), lfieldDefinition.getType());

                for (Iterator k=values2.iterator(); !result && k.hasNext(); ) {
                    Object object2 = convert(k.next(), rfieldDefinition.getType());

                    if (compare(operator, object1, object2)) {
                        result = true;
                        break;
                    }
                }
            }

            if (!result) return false;
/*
            log.debug("Comparing "+relationship+":");
            log.debug(" - "+lhs+": "+values1+" ("+lfieldDefinition.getType()+")");
            log.debug(" - "+rhs+": "+values2+" ("+rfieldDefinition.getType()+")");
            log.debug("Result: "+result);
*/
        }

        return true;
    }

    public Object convert(Object value, String type) {
        if ("VARCHAR".equals(type)) {
            return value.toString();

        } else if ("INTEGER".equals(type)) {
            if (value instanceof Integer) {
                return value;
            } else {
                return new Integer(value.toString());
            }

        } else if ("DOUBLE".equals(type)) {
            if (value instanceof Double) {
                return value;
            } else {
                return new Double(value.toString());
            }

        } else {
            return value;
        }
    }

    public boolean compare(String operator, Object object1, Object object2) {

        double comparison;

        if (object1 instanceof String && object2 instanceof String) {
            String value1 = (String)object1;
            String value2 = (String)object2;

            comparison = value1.compareToIgnoreCase(value2);

        } else if (object1 instanceof Comparable && object2 instanceof Comparable) {
            Comparable value1 = (Comparable)object1;
            Comparable value2 = (Comparable)object2;

            comparison = value1.compareTo(value2);

        } else {

            comparison = object1.equals(object2) ? 0 : 1;

            if ("=".equals(operator) && comparison == 0) {
                return true;

            } else if ("<>".equals(operator) && comparison != 0) {
                return true;

            } else {
                return false;
            }
        }

        if ("=".equals(operator) && comparison == 0) {
            return true;

        } else if ("<".equals(operator) && comparison < 0) {
            return true;

        } else if ("<=".equals(operator) && comparison <= 0) {
            return true;

        } else if (">".equals(operator) && comparison > 0) {
            return true;

        } else if (">=".equals(operator) && comparison >= 0) {
            return true;

        } else if ("<>".equals(operator) && comparison != 0) {
            return true;

        } else {
            return false;
        }
    }

}
