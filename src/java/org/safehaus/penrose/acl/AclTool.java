/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.acl;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.PenroseConnection;
import org.ietf.ldap.LDAPDN;

/**
 * @author Endi S. Dewata
 */
public class AclTool {

    public Penrose penrose;

    public AclTool(Penrose penrose) {
        this.penrose = penrose;
    }

    public boolean isAllowedToRead(PenroseConnection connection, EntryDefinition entry, String attributeName) {
    	
    	if (connection == null) return true;

        String rootDn = LDAPDN.normalize(penrose.getRootDn());
    	if (rootDn.equals(connection.getBindDn())) return true;

        if (entry.getDn().equals(connection.getBindDn())) return true;

        if (attributeName.equals("userPassword")) return false;

        return true;
    }
}
