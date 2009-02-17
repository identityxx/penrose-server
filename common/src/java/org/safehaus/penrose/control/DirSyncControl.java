package org.safehaus.penrose.control;

import com.novell.ldap.asn1.*;

/**
 * @author Endi Sukma Dewata
 */
public class DirSyncControl extends Control {

    public final static String OID = "1.2.840.113556.1.4.841";
    public final static byte[] EMPTY_COOKIE = new byte[0];

    protected int parentsFirst;
    protected int maxReturnLength;
    protected byte[] cookie;

    public DirSyncControl() throws Exception {
        super(OID, null, true);

        this.parentsFirst = 1;
        this.maxReturnLength = Integer.MAX_VALUE;
        this.cookie = EMPTY_COOKIE;

        encodeValue();
    }

    public DirSyncControl(
            int parentsFirst,
            int maxReturnLength,
            byte[] cookie,
            boolean critical
    ) throws Exception {
        super(OID, null, critical);

        this.parentsFirst = parentsFirst;
        this.maxReturnLength = maxReturnLength;
        this.cookie = cookie;

        encodeValue();
    }

    public void encodeValue() throws Exception {

        ASN1Sequence sequence = new ASN1Sequence();

        sequence.add(new ASN1Integer(parentsFirst));
        sequence.add(new ASN1Integer(maxReturnLength));
        sequence.add(new ASN1OctetString(cookie == null ? EMPTY_COOKIE : cookie));

        LBEREncoder encoder = new LBEREncoder();
        value = sequence.getEncoding(encoder);
    }

    public void decodeValue() throws Exception {

        LBERDecoder decoder = new LBERDecoder();

        ASN1Sequence sequence = (ASN1Sequence)decoder.decode(value);

        ASN1Integer parentsFirst = (ASN1Integer)sequence.get(0);
        this.parentsFirst = parentsFirst.intValue();

        ASN1Integer maxReturnLength = (ASN1Integer)sequence.get(1);
        this.maxReturnLength = maxReturnLength.intValue();

        ASN1OctetString cookie = (ASN1OctetString)sequence.get(2);
        this.cookie = cookie.byteValue();
    }

    public int getParentsFirst() {
        return parentsFirst;
    }

    public void setParentsFirst(int parentsFirst) {
        this.parentsFirst = parentsFirst;
    }

    public int getMaxReturnLength() {
        return maxReturnLength;
    }

    public void setMaxReturnLength(int maxReturnLength) {
        this.maxReturnLength = maxReturnLength;
    }

    public byte[] getCookie() {
        return cookie;
    }

    public void setCookie(byte[] cookie) {
        this.cookie = cookie;
    }
}