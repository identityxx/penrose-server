package org.safehaus.penrose.backend;

import org.safehaus.penrose.ldap.Response;
import org.safehaus.penrose.control.Control;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseResponse implements com.identyx.javabackend.Response {

    Response response;

    public PenroseResponse(Response response) {
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    public void addControl(com.identyx.javabackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.addControl(penroseControl.getControl());
    }

    public void removeControl(com.identyx.javabackend.Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.removeControl(penroseControl.getControl());
    }

    public void setControls(Collection controls) throws Exception {
        Collection<Control> list = new ArrayList<Control>();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            PenroseControl control = (PenroseControl)i.next();
            list.add(control.getControl());
        }
        response.setControls(list);
    }

    public Collection getControls() throws Exception {
        Collection list = new ArrayList();
        for (Iterator i= response.getControls().iterator(); i.hasNext(); ) {
            Control control = (Control)i.next();
            list.add(new PenroseControl(control));
        }
        return list;
    }
}
