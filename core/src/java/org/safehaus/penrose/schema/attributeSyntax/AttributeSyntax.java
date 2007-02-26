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

import java.lang.reflect.Field;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class AttributeSyntax {

    public Logger log = LoggerFactory.getLogger(AttributeSyntax.class);

    public final static AttributeSyntax ACI_ITEM = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.1", "ACI Item", false);

    public final static AttributeSyntax ACCESS_POINT = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.2", "Access Point", true);

    public final static AttributeSyntax ATTRIBUTE_TYPE_DESCRIPTION = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.3", "Attribute Type Description", true);

    public final static AttributeSyntax AUDIO = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.4", "Audio", false);

    public final static AttributeSyntax BINARY = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.5", "Binary", false);

    public final static AttributeSyntax BIT_STRING = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.6", "Bit String", true);

    public final static AttributeSyntax BOOLEAN = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.7", "Boolean", true);

    public final static AttributeSyntax CERTIFICATE = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.8", "Certificate", false);

    public final static AttributeSyntax CERTIFICATE_LIST = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.9", "Certificate List", false);

    public final static AttributeSyntax CERTIFICATE_PAIR = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.10", "Certificate Pair", false);

    public final static AttributeSyntax COUNTRY_STRING = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.11", "Country String", true);

    public final static AttributeSyntax DN = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.12", "DN", true);

    public final static AttributeSyntax DATA_QUALITY_SYNTAX = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.13", "Data Quality Syntax", true);

    public final static AttributeSyntax DELIVERY_METHOD = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.14", "Delivery Method", true);

    public final static AttributeSyntax DIRECTORY_STRING = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.15", "Directory String", true);

    public final static AttributeSyntax IA5_STRING = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.26", "IA5 String", true);

    public final static AttributeSyntax INTEGER = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.27", "Integer", true);

    public final static AttributeSyntax NUMERIC_STRING = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.36", "Numeric String", true);

    public final static AttributeSyntax OID = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.38", "OID", true);

    public final static AttributeSyntax OCTET_STRING = new AttributeSyntax(
            "1.3.6.1.4.1.1466.115.121.1.40", "Octet String", true);

    public static Map attributeSyntaxes = new TreeMap();

    static {
        try {
            Field fields[] = AttributeSyntax.class.getFields();
            for (int i=0; i<fields.length; i++) {
                Field field = fields[i];
                if (!AttributeSyntax.class.equals(field.getType())) continue;

                AttributeSyntax as = (AttributeSyntax)field.get(null);
                attributeSyntaxes.put(as.getOid(), as);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Collection getAttributeSyntaxes() {
        return attributeSyntaxes.values();
    }
    
    public static AttributeSyntax getAttributeSyntax(String oid) {
        return (AttributeSyntax)attributeSyntaxes.get(oid);
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
