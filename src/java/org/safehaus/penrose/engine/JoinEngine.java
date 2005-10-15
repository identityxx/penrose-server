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

import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.mapping.Relationship;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JoinEngine {

    public JoinEngine(Engine engine) {

    }

    public Collection join(Collection list1, Collection list2, Collection relationships) throws Exception {
        Collection results = new ArrayList();

        //log.debug("Left join:");
        for (Iterator i=list1.iterator(); i.hasNext(); ) {
            AttributeValues av1 = (AttributeValues)i.next();
            //log.debug(" - "+av1);

            boolean found = false;

            for (Iterator j=list2.iterator(); j.hasNext(); ) {
                AttributeValues av2 = (AttributeValues)j.next();
                //log.debug("    - "+av2);

                if (evaluate(relationships, av1, av2)) {

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

        }

        return results;
    }

    public Collection leftJoin(Collection list1, Collection list2, Collection relationships) throws Exception {
        Collection results = new ArrayList();

        //log.debug("Left join:");
        for (Iterator i=list1.iterator(); i.hasNext(); ) {
            AttributeValues av1 = (AttributeValues)i.next();
            //log.debug(" - "+av1);

            boolean found = false;

            for (Iterator j=list2.iterator(); j.hasNext(); ) {
                AttributeValues av2 = (AttributeValues)j.next();
                //log.debug("    - "+av2);

                if (evaluate(relationships, av1, av2)) {

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

    public boolean evaluate(Collection relationships, AttributeValues sv1, AttributeValues sv2) {
        if (relationships == null) return true;

        for (Iterator i=relationships.iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            //log.debug("Comparing "+relationship+":");

            String lhs = relationship.getLhs();
            String operator = relationship.getOperator();
            String rhs = relationship.getRhs();

            Collection values1 = sv1.get(lhs);
            Collection values2 = sv2.get(rhs);

            if (values1 == null && values2 == null) {
                values1 = sv1.get(rhs);
                values2 = sv2.get(lhs);
            }

            //log.debug(" - "+lhs+": "+values1);
            //log.debug(" - "+rhs+": "+values2);

            if (values1 == null || values2 == null) return false;

            boolean result = false;

            for (Iterator j=values1.iterator(); !result && j.hasNext(); ) {
                String value1 = j.next().toString();

                for (Iterator k=values2.iterator(); !result && k.hasNext(); ) {
                    String value2 = k.next().toString();

                    int comparison = value1.compareToIgnoreCase(value2);

                    if ("=".equals(operator) && comparison == 0) {
                        result = true;
                        break;

                    } else if ("<".equals(operator) && comparison < 0) {
                        result = true;
                        break;

                    } else if ("<=".equals(operator) && comparison <= 0) {
                        result = true;
                        break;

                    } else if (">".equals(operator) && comparison > 0) {
                        result = true;
                        break;

                    } else if (">=".equals(operator) && comparison >= 0) {
                        result = true;
                        break;

                    } else if ("<>".equals(operator) && comparison != 0) {
                        result = true;
                        break;

                    }
                }
            }

            //log.debug("Result: "+result);

            if (!result) return false;
        }

        return true;
    }

}
