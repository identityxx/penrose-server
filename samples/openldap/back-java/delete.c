/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int java_back_delete( Operation *op, SlapReply *rs ) {

    Debug( LDAP_DEBUG_TRACE, "==> java_back_delete()\n", 0, 0, 0);

    JavaBackend *be = (JavaBackend *)op->o_bd->be_private;
    Connection *conn = (Connection *)op->o_conn;

    JNIEnv *env;
    jint res;
  
    jstring dn;

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);
    if (res < 0) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_delete(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    dn = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);

    Debug( LDAP_DEBUG_TRACE, "backend.delete(\"%s\");\n", dn, 0, 0);

    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendDelete, conn->c_connid, dn);

    if (exceptionOccurred(env)) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_delete(): Failed deleting entry %s.\n", dn, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }       

    rs->sr_err = res;
    send_ldap_result( op, rs );

    return 0;
}

