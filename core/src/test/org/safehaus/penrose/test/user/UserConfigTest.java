package org.safehaus.penrose.test.user;

import junit.framework.TestCase;
import org.safehaus.penrose.user.UserConfig;

import java.util.Arrays;

/**
 * @author Endi S. Dewata
 */
public class UserConfigTest extends TestCase {

    public void testClone() throws Exception {
        UserConfig u1 = new UserConfig();
        u1.setDn("uid=admin,ou=system");
        u1.setPassword("secret");

        UserConfig u2 = (UserConfig)u1.clone();
        assertEquals(u1.getDn(), u2.getDn());
        assertTrue(Arrays.equals(u1.getPassword(), u2.getPassword()));
    }

    public void testEquals() {
        UserConfig u1 = new UserConfig();
        u1.setDn("uid=admin,ou=system");
        u1.setPassword("secret");

        UserConfig u2 = new UserConfig();
        u2.setDn("uid=admin,ou=system");
        u2.setPassword("secret");

        assertEquals(u2, u2);
    }
}
