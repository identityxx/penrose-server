/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int java_back_bind( Operation *op, SlapReply *rs ) {

    Debug( LDAP_DEBUG_TRACE, "==> java_back_bind()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;

    jint res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );

        return LDAP_OPERATIONS_ERROR;
    }

    jstring dn = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);
    jstring cred = (*env)->NewStringUTF(env, op->orb_cred.bv_val);
  
    // backend.bind(connectionId, dn, normalizedDn, password);
    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendBind, conn->c_connid, dn, cred);

    if (res != LDAP_SUCCESS) {
        rs->sr_err = res;
        send_ldap_result( op, rs );
    }

    return res;
}

int java_back_unbind( Operation *op, SlapReply *rs ) {

    Debug( LDAP_DEBUG_TRACE, "==> java_back_unbind()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;

    jint res;

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_add(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );

        return LDAP_OPERATIONS_ERROR;
    }

    // backend.unbind();
    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendUnbind, conn->c_connid);

    if (res != LDAP_SUCCESS) {
        rs->sr_err = res;
        send_ldap_result( op, rs );
    }

    return res;
}


