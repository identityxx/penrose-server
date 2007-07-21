/**
 * Copyright (c) 2000-2006, Identyx Corporation.
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

import java.util.*;

import org.safehaus.penrose.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class TransformEngine {

    public static Logger log = LoggerFactory.getLogger(TransformEngine.class);

    public Engine engine;

    public int joinDebug = 0;
    public static int crossProductDebug = 0;

    public TransformEngine(Engine engine) {
        this.engine = engine;
    }

    /**
     * Convert attribute values into rows.
     *
     * Input: Attributes(attr1=[a, b, c], attr2=[1, 2, 3])
     * Output: List(RDN(attr1=a, attr2=1), RDN(attr1=a, attr2=2), ... )
     *
     * @param attributes Attributes
     * @return collection of RDNs
     */
    public static Collection<RDN> convert(Attributes attributes) {
        List<String> names = new ArrayList<String>(attributes.getNames());
        List<RDN> results = new ArrayList<RDN>();
        RDNBuilder rb = new RDNBuilder();

        convert(attributes, names, 0, rb, results);

        return results;
    }

    public static void convert(
            Attributes attributes,
            List names,
            int pos,
            RDNBuilder rb,
            Collection<RDN> results
    ) {

        if (pos < names.size()) {
            String name = (String)names.get(pos);
            Attribute attribute = attributes.get(name);

            for (Object value : attribute.getValues()) {
                rb.set(name, value);
                convert(attributes, names, pos + 1, rb, results);
            }

        } else if (!rb.isEmpty()) {
            RDN rdn = rb.toRdn();
            results.add(rdn);
        }
    }
}
