/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.schema.matchingRule;

/**
 * @author Endi S. Dewata
 */
public class CaseIgnoreOrderingMatchingRule extends OrderingMatchingRule {

    public int compare(Object object1, Object object2) throws Exception {
        
        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug("Comparing:");
            log.debug(" - "+object1+" ("+object1.getClass().getSimpleName()+")");
            log.debug(" - "+object2+" ("+object2.getClass().getSimpleName()+")");
        }

        if (object1 instanceof String && object2 instanceof String) {
            String s1 = (String)object1;
            String s2 = (String)object2;

            return s1.compareToIgnoreCase(s2);
        }

        return super.compare(object1, object2);
    }

}
