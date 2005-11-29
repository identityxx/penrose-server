/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
import java.util.Iterator;
import java.util.ArrayList;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.PenroseSession;
import org.safehaus.penrose.SearchResults;
import org.safehaus.penrose.schema.SchemaConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.config.DefaultPenroseConfig;
import org.ietf.ldap.*;
import org.apache.log4j.*;

/**
 * @author Endi S. Dewata
 */
public class Demo {
	
	public final static String SUFFIX = "dc=Example,dc=com";

	public void run () throws Exception {

        ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
        BasicConfigurator.configure(appender);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.toLevel("OFF"));

        Logger logger = Logger.getLogger("org.safehaus.penrose");
        logger.setLevel(Level.toLevel("INFO"));

        PenroseConfig penroseConfig = new DefaultPenroseConfig();

        SchemaConfig schemaConfig = new SchemaConfig("samples/schema/example.schema");
        penroseConfig.addSchemaConfig(schemaConfig);

        PartitionConfig partitionConfig = new PartitionConfig("example", "samples/conf");
        penroseConfig.addPartitionConfig(partitionConfig);

		Penrose penrose = new Penrose(penroseConfig);
		penrose.start();

        PenroseSession session = penrose.newSession();
        session.bind("uid=admin,ou=system", "secret");

        ArrayList attributeNames = new ArrayList();

        SearchResults results = session.search(
                "ou=Categories,"+SUFFIX,
                LDAPConnection.SCOPE_ONE,
                LDAPSearchConstraints.DEREF_ALWAYS,
                "(objectClass=*)",
                attributeNames);

        for (Iterator i = results.iterator(); i.hasNext();) {
            LDAPEntry entry = (LDAPEntry) i.next();
            System.out.println(toString(entry));
        }

        session.unbind();

        session.close();
        penrose.stop();
    }

    public String toString(LDAPEntry entry) throws Exception {

        StringBuffer sb = new StringBuffer();
        sb.append("dn: "+entry.getDN()+"\n");

        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();

            String name = attribute.getName();

            String values[] = attribute.getStringValueArray();

            for (int j=0; j<values.length; j++) {
                sb.append(name+": "+values[j]+"\n");
            }
        }

        return sb.toString();
    }

    public static void main(String args[]) throws Exception {
        Demo demo = new Demo();
        demo.run();
    }
}
