package org.safehaus.penrose.backend;

import org.safehaus.penrose.control.Control;

/**
 * @author Endi S. Dewata
 */
public class PenroseControl implements com.identyx.javabackend.Control {

    Control control;

    public PenroseControl(Control control) {
        this.control = control;
    }

    public String getOid() throws Exception {
        return control.getOid();
    }

    public byte[] getValue() throws Exception {
        return control.getValue();
    }

    public boolean isCritical() throws Exception {
        return control.isCritical();
    }

    public Control getControl() throws Exception {
        return control;
    }
}
