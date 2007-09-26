package org.safehaus.penrose.ldap;

import org.safehaus.penrose.control.Control;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class Request implements Cloneable {

    protected Collection<Control> controls = new ArrayList<Control>();

    public Request() {
    }

    public Request(Request request) {
        controls.addAll(request.controls);
    }
    
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

    public int hashCode() {
        return controls.hashCode();
    }

    private boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Request request = (Request)object;
        if (!equals(controls, request.controls)) return false;

        return true;
    }

    public Object clone() throws CloneNotSupportedException {
        Request request = (Request)super.clone();

        request.controls = new ArrayList<Control>();
        request.controls.addAll(controls);

        return request;
    }
}
