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
package org.safehaus.penrose.ldapbackend.apacheds;

import org.safehaus.penrose.ldapbackend.Attribute;
import org.safehaus.penrose.ldapbackend.Attributes;
import org.safehaus.penrose.ldapbackend.DN;

import javax.naming.directory.SearchResult;
import javax.naming.directory.BasicAttribute;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EntryTool {

    public static SearchResult createSearchResult(org.safehaus.penrose.ldapbackend.SearchResult result) throws Exception {

        DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        javax.naming.directory.Attributes attrs = new javax.naming.directory.BasicAttributes();

        for (Attribute attribute : attributes.getAll()) {

            String name = attribute.getName();
            Collection values = attribute.getValues();

            javax.naming.directory.Attribute attr = new BasicAttribute(name);
            for (Object value : values) {
                //String className = value.getClass().getName();
                //className = className.substring(className.lastIndexOf(".")+1);
                //log.debug(" - "+name+": "+value+" ("+className+")");

                if (value instanceof byte[]) {
                    attr.add(value);

                } else {
                    attr.add(value.toString());
                }
            }

            attrs.put(attr);
        }

        return new SearchResult(dn.toString(), result, attrs);
    }
}
