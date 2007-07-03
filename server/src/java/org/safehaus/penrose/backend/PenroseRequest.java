package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Request;
import org.safehaus.penrose.control.Control;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseRequest implements com.identyx.javabackend.Request {

    Request request;

    public PenroseRequest(Request request) {
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

    public void addControl(com.identyx.javabackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        request.addControl(penroseControl.getControl());
    }

    public void removeControl(com.identyx.javabackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        request.removeControl(penroseControl.getControl());
    }

    public void setControls(Collection controls) throws Exception {
        Collection<Control> list = new ArrayList<Control>();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            PenroseControl control = (PenroseControl)i.next();
            list.add(control.getControl());
        }
        request.setControls(list);
    }

    public Collection getControls() throws Exception {
        Collection list = new ArrayList();
        for (Iterator i= request.getControls().iterator(); i.hasNext(); ) {
            org.safehaus.penrose.control.Control control = (org.safehaus.penrose.control.Control)i.next();
            list.add(new PenroseControl(control));
        }
        return list;
    }
}
