/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int java_back_compare( Operation *op, SlapReply *rs ) {

    Debug( LDAP_DEBUG_TRACE, "==> java_back_compare()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;

    AttributeAssertion *ava = (AttributeAssertion *)op->orc_ava;

    jint res;

    jstring dn;
    jstring name;
    jstring value;

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_compare(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    dn = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);
    name = (*env)->NewStringUTF(env, ava->aa_desc->ad_cname.bv_val);
    value = (*env)->NewStringUTF(env, ava->aa_value.bv_val);
   
    // backend.compare(dn, name, value);
    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendCompare,
        conn->c_connid, dn, name, value);

    if (exceptionOccurred(env)) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_compare(): Failed to compare \"%s\" %s:%s.\n", dn, name, value);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }       

    rs->sr_err = res;
    send_ldap_result( op, rs );

    Debug( LDAP_DEBUG_TRACE, "<== java_back_compare(): %s\n", res == LDAP_COMPARE_TRUE ? "TRUE" : "FALSE", 0, 0);

    return LDAP_SUCCESS;
}


