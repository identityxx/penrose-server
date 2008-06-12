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
package org.safehaus.penrose.pipeline;

import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.ldap.SearchReference;
import org.safehaus.penrose.control.Control;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Pipeline extends SearchResponse {

    public SearchResponse parent;

    public Pipeline(SearchResponse parent) {
        this.parent = parent;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Response
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void addControl(Control control) {
        parent.addControl(control);
    }

    public void setControls(Collection<Control> controls) {
        parent.setControls(controls);
    }

    public void removeControl(Control control) {
        parent.removeControl(control);
    }

    public Collection<Control> getControls() {
        return parent.getControls();
    }

    public void setException(LDAPException e) {
        parent.setException(e);
    }

    public void setException(Exception e) {
        parent.setException(e);
    }

    public LDAPException getException() {
        return parent.getException();
    }

    public void setReturnCode(int returnCode) {
        parent.setReturnCode(returnCode);
    }

    public int getReturnCode() {
        return parent.getReturnCode();
    }

    public String getErrorMessage() {
        return parent.getErrorMessage();
    }

    public String getMessage() {
        return parent.getMessage();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // SearchResponse
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(SearchResult result) throws Exception {
        parent.add(result);
    }

    public boolean hasNext() throws Exception {
        return parent.hasNext();
    }
    
    public SearchResult next() throws Exception {
        return parent.next();
    }

    public Collection<SearchResult> getAll() {
        return parent.getAll();
    }

    public void add(SearchReference reference) throws Exception {
        parent.add(reference);
    }

    public Collection<SearchReference> getReferences() {
        return parent.getReferences();
    }

    public int waitFor() {
        return parent.waitFor();
    }

    public void close() throws Exception {
        parent.close();
    }

    public boolean isClosed() {
        return parent.isClosed();
    }

    public long getTotalCount() {
        return parent.getTotalCount();
    }

    public void setSizeLimit(long sizeLimit) {
        parent.setSizeLimit(sizeLimit);
    }

    public long getSizeLimit() {
        return parent.getSizeLimit();
    }

    public void setBufferSize(long bufferSize) {
        parent.setBufferSize(bufferSize);
    }

    public long getBufferSize() {
        return parent.getBufferSize();
    }

    public SearchResponse getParent() {
        return parent;
    }

    public void setParent(SearchResponse parent) {
        this.parent = parent;
    }
}
