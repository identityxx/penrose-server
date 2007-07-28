package org.safehaus.penrose.example.tomcat;

import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.service.Services;
import org.safehaus.penrose.Penrose;

import javax.servlet.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class DemoServlet extends GenericServlet {

    PenroseServer penroseServer;

    public void init(ServletConfig servletConfig) throws ServletException {
        super.init(servletConfig);

        try {
            String home = servletConfig.getInitParameter("penrose.home");

            System.out.println("Starting Penrose Server at "+home+"...");
            penroseServer = new PenroseServer(home);
            penroseServer.start();

            System.out.println("Penrose Server started.");

        } catch (Exception e) {
            System.out.println("Failed starting Penrose Server: "+e.getMessage());
            throw new ServletException(e);
        }

    }

    public void destroy() {
        try {
            System.out.println("Stopping Penrose Server...");
            penroseServer.stop();

            System.out.println("Penrose Server stopped.");

        } catch (Exception e) {
            System.out.println("Failed stopping Penrose Server: "+e.getMessage());
        }
    }

    public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException {
        servletResponse.setContentType("text/plain");
        PrintWriter out = servletResponse.getWriter();

        try {
            out.println("Penrose Server:");
            out.println(" - version: "+Penrose.PRODUCT_VERSION);
            out.println(" - status: "+penroseServer.getStatus());
            out.println();

            out.println("Penrose Services:");
            Services serviceManager = penroseServer.getServices();
            Collection serviceNames = serviceManager.getServiceNames();
            for (Iterator i=serviceNames.iterator(); i.hasNext(); ) {
                String serviceName = (String)i.next();
                out.println(" - "+serviceName+": "+serviceManager.getStatus(serviceName));
            }

        } catch (Exception e) {
            System.out.println("Error: "+e.getMessage());
            throw new ServletException(e);
        }

        out.close();
    }
}
