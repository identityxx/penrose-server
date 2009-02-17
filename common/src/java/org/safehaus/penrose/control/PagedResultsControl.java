package org.safehaus.penrose.control;

import com.novell.ldap.asn1.*;

/**
 * @author Endi Sukma Dewata
 */
public class PagedResultsControl extends Control {

    public final static String OID = "1.2.840.113556.1.4.319";
    public final static byte[] EMPTY_COOKIE = new byte[0];

    protected int pageSize;
    protected byte[] cookie;

    public PagedResultsControl(Control control) throws Exception {
        super(control);

        decodeValue();
    }

    public PagedResultsControl(String oid, byte[] value, boolean critical) throws Exception {
        super(oid, value, critical);

        decodeValue();
    }

    public PagedResultsControl(int pageSize, boolean critical) throws Exception {
        super(OID, null, critical);

        this.pageSize = pageSize;
        this.cookie = EMPTY_COOKIE;
        
        encodeValue();
    }

    public void encodeValue() throws Exception {

        ASN1Sequence sequence = new ASN1Sequence();

        sequence.add(new ASN1Integer(pageSize));
        sequence.add(new ASN1OctetString(cookie == null ? EMPTY_COOKIE : cookie));

        LBEREncoder encoder = new LBEREncoder();
        value = sequence.getEncoding(encoder);
    }

    public void decodeValue() throws Exception {

        LBERDecoder decoder = new LBERDecoder();

        ASN1Sequence sequence = (ASN1Sequence)decoder.decode(value);

        ASN1Integer pageSize = (ASN1Integer)sequence.get(0);
        this.pageSize = pageSize.intValue();

        ASN1OctetString cookie = (ASN1OctetString)sequence.get(1);
        this.cookie = cookie.byteValue();
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public byte[] getCookie() {
        return cookie;
    }

    public void setCookie(byte[] cookie) {
        this.cookie = cookie;
    }
}
