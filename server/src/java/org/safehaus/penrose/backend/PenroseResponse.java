package org.safehaus.penrose.backend;

import org.safehaus.penrose.session.Response;
import com.identyx.javabackend.Control;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class PenroseResponse implements com.identyx.javabackend.Response {

    private Response response;

    public PenroseResponse(Response response) {
        this.response = response;
    }

    public Response getResponse() {
        return response;
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    public void addControl(Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.addControl(penroseControl.getControl());
    }

    public void removeControl(Control control) throws Exception {
        PenroseControl penroseControl = (PenroseControl)control;
        response.removeControl(penroseControl.getControl());
    }

    public void setControls(Collection controls) throws Exception {
        Collection list = new ArrayList();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            PenroseControl control = (PenroseControl)i.next();
            list.add(control.getControl());
        }
        response.setControls(list);
    }

    public Collection getControls() throws Exception {
        Collection list = new ArrayList();
        for (Iterator i= response.getControls().iterator(); i.hasNext(); ) {
            org.safehaus.penrose.control.Control control = (org.safehaus.penrose.control.Control)i.next();
            list.add(new PenroseControl(control));
        }
        return list;
    }
}
