package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class Request {

    protected Collection<Control> controls = new ArrayList<Control>();

    public void addControl(Control control) {
        controls.add(control);
    }

    public void removeControl(Control control) {
        controls.remove(control);
    }

    public void setControls(Collection<Control> controls) {
        this.controls.clear();
        if (controls != null) this.controls.addAll(controls);
    }

    public Collection<Control> getControls() {
        return controls;
    }
}
