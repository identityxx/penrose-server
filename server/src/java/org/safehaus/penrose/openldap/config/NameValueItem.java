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
package org.safehaus.penrose.openldap.config;

import java.io.StringReader;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


/**
 * @author Administrator
 */
public class NameValueItem extends ConfigurationItem {

    Logger log = LoggerFactory.getLogger(getClass());

    protected String name;
    protected String whitespace;
    protected String value;

    /**
     *
     */
    public NameValueItem() {
        super();
    }

    /**
     * @param originalText
     */
    public NameValueItem(String originalText) {
        super(originalText);
        parse(originalText);
    }

    /**
     *
     * @param s
     */
    protected void parse(String s) {
        StringReader sr = new StringReader(s);
        SlapdParser parser = new SlapdParser(sr);
        try {
            String[] result = parser.NameValue();
            this.name = result[0];
            this.whitespace = result[1];
            this.value = result[2];
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
        modify();
    }
    public String getValue() {
        return value;
    }
    public void setValue(String value) {
        this.value = value;
        modify();
    }
    public String getWhitespace() {
        return whitespace;
    }
    public void setWhitespace(String whitespace) {
        this.whitespace = whitespace;
        modify();
    }

    protected void modify() {
        modifiedText = name + whitespace;
        if (value != null) {
            modifiedText +=
            ("".equals(value) || value.indexOf(" ")>0 ? "\"" + value + "\"" : value);
        }
    }
}
