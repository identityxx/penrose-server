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
package org.safehaus.penrose.schema.attributeSyntax;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Endi S. Dewata
 */
public class AttributeSyntax {

    public static Logger log = LoggerFactory.getLogger(AttributeSyntax.class);
    public static boolean debug = log.isDebugEnabled();

    public static Map<String,AttributeSyntax> attributeSyntaxes = new TreeMap<String,AttributeSyntax>();

    static {

        try {
            //if (debug) {
                //log.debug("----------------------------------------------------------------------------------");
                //log.debug("Attribute syntaxes:");
            //}

            ClassLoader cl = AttributeSyntax.class.getClassLoader();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(
                        cl.getResourceAsStream(
                                "org/safehaus/penrose/schema/attributeSyntax/AttributeSyntax.properties"
                        )
                    )
            );

            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) continue;
                if (line.startsWith("#")) continue;

                //if (debug) log.debug("Parsing ["+line+"]");
                int i = line.lastIndexOf(' ');
                String oid = line.substring(i+1);
                //if (debug) log.debug(" - OID            : "+oid);

                while (line.charAt(i) == ' ') i--;

                boolean humanReadable = line.charAt(i) == 'Y';
                //if (debug) log.debug(" - Human Readable : "+humanReadable);

                String description = line.substring(0, i).trim();
                //if (debug) log.debug(" - Description    : "+description);

                //if (debug) log.debug(" - "+oid+": "+description+(humanReadable ? " [Y]" : ""));

                AttributeSyntax attributeSyntax = new AttributeSyntax(oid, description, humanReadable);
                attributeSyntaxes.put(oid, attributeSyntax);
            }

            in.close();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Collection getAttributeSyntaxes() {
        return attributeSyntaxes.values();
    }
    
    public static AttributeSyntax getAttributeSyntax(String oid) {
        return attributeSyntaxes.get(oid);
    }

    public String oid;
    public String description;
    public boolean humanReadable;

    public AttributeSyntax(String oid, String description, boolean humanReadable) {
        this.oid = oid;
        this.description = description;
        this.humanReadable = humanReadable;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isHumanReadable() {
        return humanReadable;
    }

    public void setHumanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
    }
}
