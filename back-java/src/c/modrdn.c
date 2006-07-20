/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int java_back_modrdn( Operation *op, SlapReply *rs ) {

    Debug( LDAP_DEBUG_TRACE, "==> java_back_modrdn()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;

    jint res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_modrdn(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    jstring dn = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);
    jstring newRdn = (*env)->NewStringUTF(env, op->orr_newrdn.bv_val);

    Debug( LDAP_DEBUG_TRACE, "backend.modrdn(\"%s\", \"%s\")\n", dn, newRdn, 0);

    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendModRdn, conn->c_connid, dn, newRdn);

    jthrowable exc = (*env)->ExceptionOccurred(env);

    if (exc) {
        Debug( LDAP_DEBUG_TRACE, "<== java_back_modrdn(): Failed renaming entry %s.\n", dn, 0, 0);

        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );

        return LDAP_OPERATIONS_ERROR;
    }

    rs->sr_err = res;
    send_ldap_result( op, rs );

    return 0;
}

