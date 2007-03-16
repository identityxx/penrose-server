package org.safehaus.penrose.session;

import org.safehaus.penrose.control.Control;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class Response {

    protected Collection controls   = new ArrayList();

    public void addControl(Control control) {
        controls.add(control);
    }

    public void removeControl(Control control) {
        controls.remove(control);
    }

    public void setControls(Collection controls) {
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }

    public Collection getControls() {
        return controls;
    }
}
