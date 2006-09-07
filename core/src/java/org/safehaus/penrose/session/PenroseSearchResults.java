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
package org.safehaus.penrose.session;

import org.safehaus.penrose.pipeline.Pipeline;
import org.ietf.ldap.LDAPException;

/**
 * @author Endi S. Dewata
 */
public class PenroseSearchResults extends Pipeline {

    int returnCode = LDAPException.SUCCESS;

    public PenroseSearchResults() {
    }

    public synchronized int getReturnCode() {
        while (!isClosed()) {
            try {
                wait();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        return returnCode;
    }

    public void resetReturnCode() {
        this.returnCode = LDAPException.SUCCESS;
    }

    public void setReturnCode(int returnCode) {
        if (this.returnCode != LDAPException.SUCCESS) return;
        this.returnCode = returnCode;
    }
}
