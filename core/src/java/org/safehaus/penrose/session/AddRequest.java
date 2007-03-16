package org.safehaus.penrose.session;

import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.AttributeValues;

import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;

/**
 * @author Endi S. Dewata
 */
public class AddRequest extends Request {

    protected DN dn;
    protected AttributeValues attributeValues;

    public DN getDn() {
        return dn;
    }

    public void setDn(String dn) throws Exception {
        this.dn = new DN(dn);
    }

    public void setDn(DN dn) {
        this.dn = dn;
    }

    public AttributeValues getAttributeValues() {
        return attributeValues;
    }

    public void setAttributeValues(Attributes attributes) throws Exception {
        attributeValues = new AttributeValues();

        for (NamingEnumeration ne = attributes.getAll(); ne.hasMore(); ) {
            Attribute attribute = (Attribute)ne.next();
            String name = attribute.getID();

            for (NamingEnumeration ne2 = attribute.getAll(); ne2.hasMore(); ) {
                Object value = ne2.next();
                attributeValues.add(name, value);
            }
        }
    }

    public void setAttributeValues(AttributeValues attributeValues) {
        this.attributeValues = attributeValues;
    }
}
