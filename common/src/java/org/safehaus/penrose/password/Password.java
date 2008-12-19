package org.safehaus.penrose.password;

import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vps.crypt.Crypt;
import org.safehaus.penrose.util.BinaryUtil;
import gnu.getopt.LongOpt;
import gnu.getopt.Getopt;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;
import java.security.MessageDigest;

import arlut.csd.crypto.MD5Crypt;
import arlut.csd.crypto.Sha256Crypt;
import arlut.csd.crypto.Sha512Crypt;

/**
 * @author Endi Sukma Dewata
 */
public class Password {

    public static Logger log = LoggerFactory.getLogger(Password.class);
    public static boolean debug = log.isDebugEnabled();

    public final static String CRYPT        = "crypt";
    public final static String CRYPT_MD5    = "crypt-md5";
    public final static String CRYPT_SHA256 = "crypt-sha256";
    public final static String CRYPT_SHA512 = "crypt-sha512";

    public static String SALT_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789./";

    String encryption;
    String password;
    String salt;
    int rounds;

    public Password(String encryption, String password) throws Exception {
        this.encryption = encryption;
        this.password = password;
    }

    public Password(String encryption, String password, String salt, int rounds) throws Exception {
        this.encryption = encryption;
        this.password = password;
        this.salt     = salt;
        this.rounds   = rounds;
    }

    public String encrypt() throws Exception {
        return encrypt(password);
    }

    public String encrypt(String password) throws Exception {

        if (password == null) return null;
        if (encryption == null) return password;

        if (CRYPT.equalsIgnoreCase(encryption)) {

            salt = salt == null ? createSalt(2) : salt;

            return Crypt.crypt(salt, password);

        } else if (CRYPT_MD5.equalsIgnoreCase(encryption)) {

            salt = salt == null ? createSalt(8) : salt;

            return MD5Crypt.crypt(password, salt);

        } else if (CRYPT_SHA256.equalsIgnoreCase(encryption)) {

            salt = salt == null ? createSalt(16) : salt;

            return Sha256Crypt.Sha256_crypt(password, salt, rounds);

        } else if (CRYPT_SHA512.equalsIgnoreCase(encryption)) {

            salt = salt == null ? createSalt(16) : salt;

            return Sha512Crypt.Sha512_crypt(password, salt, rounds);

        } else {

            MessageDigest md = MessageDigest.getInstance(encryption);
            md.update(password.getBytes());

            return BinaryUtil.encode(BinaryUtil.BASE64, md.digest());
        }
    }

    public String createSalt(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<length; i++) {
            int r = (int)(Math.random() * SALT_CHARACTERS.length());
            sb.append(SALT_CHARACTERS.charAt(r));
        }
        return sb.toString();
    }

    public static boolean validate(String method, String password, String hash) throws Exception {

        if (CRYPT.equalsIgnoreCase(method)) {

            if (hash.startsWith("$1$")) {
                method = CRYPT_MD5;

            } else if (hash.startsWith("$5$")) {
                method = CRYPT_SHA256;

            } else if (hash.startsWith("$6$")) {
                method = CRYPT_SHA512;
            }
        }

        String newHash = new Password(method, password, hash, 0).encrypt();

        if (debug) {
            log.debug("Validating passwords:");
            if (method != null) log.debug(" - encryption: ["+method+"]");

            log.debug(" - supplied  : ["+newHash+"]");
            log.debug(" - stored    : ["+hash+"]");
        }

        boolean result = newHash.equals(hash);

        if (debug) log.debug(" - result    : "+result);

        return result;
    }

    public static void showUsage() {
        System.out.println("Usage: org.safehaus.penrose.password.Password [OPTION]... <command> [arguments]...");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -?, --help         display this help and exit");
        System.out.println("  -d                 run in debug mode");
        System.out.println("  -v                 run in verbose mode");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  crypt        <password> [salt]");
        System.out.println("  crypt-md5    <password> [salt]");
        System.out.println("  crypt-sha256 <password> [salt [rounds]]");
        System.out.println("  crypt-sha512 <password> [salt [rounds]]");
        System.out.println("  md5          <password>");
        System.out.println("  sha          <password>");
        System.out.println("  <encryption>     <password>");
    }

    public static void main(String args[]) throws Exception {

        Level level = Level.WARN;

        LongOpt[] longopts = new LongOpt[1];
        longopts[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, '?');

        Getopt getopt = new Getopt("Password", args, "-:?dv", longopts);

        Collection<String> parameters = new ArrayList<String>();
        int c;
        while ((c = getopt.getopt()) != -1) {
            switch (c) {
                case ':':
                case '?':
                    showUsage();
                    System.exit(0);
                    break;
                case 1:
                    parameters.add(getopt.getOptarg());
                    break;
                case 'd':
                    level = Level.DEBUG;
                    break;
                case 'v':
                    level = Level.INFO;
                    break;
            }
        }

        if (parameters.size() == 0) {
            showUsage();
            System.exit(0);
        }

        File clientHome = new File(System.getProperty("org.safehaus.penrose.client.home"));

        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.safehaus.penrose");

        File log4jXml = new File(clientHome, "conf"+File.separator+"log4j.xml");

        if (level.equals(Level.DEBUG)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%-20C{1} [%4L] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (level.equals(Level.INFO)) {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);

        } else if (log4jXml.exists()) {
            DOMConfigurator.configure(log4jXml.getAbsolutePath());

        } else {
            logger.setLevel(level);
            ConsoleAppender appender = new ConsoleAppender(new PatternLayout("[%d{MM/dd/yyyy HH:mm:ss}] %m%n"));
            BasicConfigurator.configure(appender);
        }

        Iterator<String> iterator = parameters.iterator();

        String method = iterator.next();
        log.debug("Method: "+method);

        String password = iterator.next();
        log.debug("Password: "+password);

        String salt = null;
        String rounds = null;

        if (iterator.hasNext()) {
            salt = iterator.next();
            log.debug("Salt: "+salt);

            if (iterator.hasNext()) {
                rounds = iterator.next();
                log.debug("Rounds: "+rounds);
            }
        }

        Password p = new Password(
                method,
                password,
                salt == null || "".equals(salt) ? null : salt,
                rounds == null || "".equals(rounds) ? 0 : Integer.parseInt(rounds)
        );

        System.out.println(p.encrypt());
    }
}
