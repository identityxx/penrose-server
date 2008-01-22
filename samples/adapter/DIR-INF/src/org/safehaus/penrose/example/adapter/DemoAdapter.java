package org.safehaus.penrose.example.adapter;

import org.safehaus.penrose.adapter.Adapter;

/**
 * @author Endi S. Dewata
 */

public class DemoAdapter extends Adapter {

    public String getConnectionClassName() {
        return DemoConnection.class.getName();
    }

    public String getSourceClassName() throws Exception {
        return DemoSource.class.getName();
    }

}
