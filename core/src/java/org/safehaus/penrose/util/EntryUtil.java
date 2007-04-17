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
package org.safehaus.penrose.util;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.entry.SourceValues;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class EntryUtil {

    Logger log = LoggerFactory.getLogger(EntryUtil.class);

    public static Attributes computeAttributes(SourceValues sourceValues) {
        Attributes attributes = new Attributes();
        for (Iterator i= sourceValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = sourceValues.get(name);
            attributes.add(new Attribute(name, values));
        }
        return attributes;
    }

    public static SourceValues computeAttributeValues(Attributes attributes) {
        SourceValues sourceValues = new SourceValues();
        for (Iterator i=attributes.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributes.getValues(name);
            sourceValues.set(name, values);
        }
        return sourceValues;
    }


}
