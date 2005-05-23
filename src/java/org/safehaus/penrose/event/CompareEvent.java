/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public class CompareEvent extends Event {

    public CompareEvent(Object source, int type, String dn, String attributeType, String attributeValue) {
        super(source, type);
    }
}
