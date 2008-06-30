package org.safehaus.penrose.control;

import com.novell.ldap.asn1.*;

/**
 * @author Endi Sukma Dewata
 */
public class DirSyncResponseControl extends Control {

    public final static String OID = "1.2.840.113556.1.4.841";
    public final static byte[] EMPTY_COOKIE = new byte[0];

    protected int flag = 1;
    protected int maxReturnLength;
    protected byte[] cookie;

    public DirSyncResponseControl(Control control) throws Exception {
        super(control);

        decodeValue();
    }

    public void encodeValue() throws Exception {

        ASN1Sequence sequence = new ASN1Sequence();

        sequence.add(new ASN1Integer(flag));
        sequence.add(new ASN1Integer(maxReturnLength));
        sequence.add(new ASN1OctetString(cookie == null ? EMPTY_COOKIE : cookie));

        LBEREncoder encoder = new LBEREncoder();
        value = sequence.getEncoding(encoder);
    }

    public void decodeValue() throws Exception {

        LBERDecoder decoder = new LBERDecoder();

        ASN1Sequence sequence = (ASN1Sequence)decoder.decode(value);

        ASN1Integer flag = (ASN1Integer)sequence.get(0);
        this.flag = flag.intValue();

        ASN1Integer maxReturnLength = (ASN1Integer)sequence.get(1);
        this.maxReturnLength = maxReturnLength.intValue();

        ASN1OctetString cookie = (ASN1OctetString)sequence.get(2);
        this.cookie = cookie.byteValue();
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
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