/**
 * @author Endi S. Dewata
 */

#include "portable.h"
#include <stdio.h>

#include "slap.h"

#include "java_back.h"

int java_back_modify( Operation *op, SlapReply *rs ) { 

    Debug( LDAP_DEBUG_TRACE, "==> java_back_modify()\n", 0, 0, 0);

    JavaBackend *java_back = (JavaBackend *)op->o_bd->be_private;
    JavaVM *jvm = java_back->jvm;
    JNIEnv *env;

    Connection *conn = (Connection *)op->o_conn;

    Modifications *modlist = (Modifications *)op->orm_modlist;

    jint res;

    jstring dn;
    jobject modifications;

    Modifications *m;

    res = (*jvm)->AttachCurrentThread(jvm, (void**)&env, NULL);

    if (res < 0) {
 
        Debug( LDAP_DEBUG_TRACE, "<== java_back_modify(): Failed attaching to JVM.\n", 0, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }

    dn = (*env)->NewStringUTF(env, op->o_req_dn.bv_val);

    Debug( LDAP_DEBUG_TRACE, "ArrayList modifications = new ArrayList();\n", 0, 0, 0);

    modifications = (*env)->NewObject(env, java_back->arrayListClass, java_back->arrayListConstructor);

    m = modlist;
    while (m) {

        Modification *mod = &m->sml_mod;

        jobject attribute;
        jstring attributeName;
        jobject modification;
        jint modOp = mod->sm_op;

        AttributeDescription *desc;
        BerValue *vals;

        desc = mod->sm_desc;

        attributeName = (*env)->NewStringUTF(env, desc->ad_cname.bv_val);

        Debug( LDAP_DEBUG_TRACE, "Attribute attribute = new BasicAttribute(\"%s\");\n", attributeName, 0, 0);

        attribute = (*env)->NewObject(env, java_back->basicAttributeClass, java_back->basicAttributeConstructor, attributeName);

        if (exceptionOccurred(env)) {

            Debug( LDAP_DEBUG_TRACE, "<== java_back_modify(): Failed creating attribute.\n", 0, 0, 0);

            rs->sr_err = LDAP_OPERATIONS_ERROR;
            send_ldap_result( op, rs );
            return LDAP_OPERATIONS_ERROR;
        }       

        vals = mod->sm_values;

        while (vals && vals->bv_val) {
            jstring attributeValue;

            attributeValue = (*env)->NewStringUTF(env, vals->bv_val);

            Debug( LDAP_DEBUG_TRACE, "attribute.add(\"%s\");\n", attributeValue, 0, 0);

            (*env)->CallBooleanMethod(env, attribute, java_back->attributeAdd, attributeValue);

            if (exceptionOccurred(env)) {

                Debug( LDAP_DEBUG_TRACE, "<== java_back_modify(): Failed adding attribute value.\n", 0, 0, 0);

                rs->sr_err = LDAP_OPERATIONS_ERROR;
                send_ldap_result( op, rs );
                return LDAP_OPERATIONS_ERROR;
            }       

            vals++;
        }

        Debug( LDAP_DEBUG_TRACE, "ModificationItem modification = new ModificationItem(%d, attribute);\n", modOp, 0, 0);

        modification = (*env)->NewObject(env, java_back->modificationItemClass, java_back->modificationItemConstructor, modOp, attribute);

        if (exceptionOccurred(env)) {

            Debug( LDAP_DEBUG_TRACE, "<== java_back_modify(): Failed creating modification.\n", 0, 0, 0);

            rs->sr_err = LDAP_OPERATIONS_ERROR;
            send_ldap_result( op, rs );
            return LDAP_OPERATIONS_ERROR;
        }       

        Debug( LDAP_DEBUG_TRACE, "modifications.add(modification);", 0, 0, 0);
 
        res = (*env)->CallIntMethod(env, modifications, java_back->arrayListAdd, modification);

        if (exceptionOccurred(env)) {

            Debug( LDAP_DEBUG_TRACE, "<== java_back_modify(): Failed adding modification to modifications.\n", 0, 0, 0);

            rs->sr_err = LDAP_OPERATIONS_ERROR;
            send_ldap_result( op, rs );
            return LDAP_OPERATIONS_ERROR;
        }       

        m = m->sml_next;
    }

    Debug( LDAP_DEBUG_TRACE, "backend.modify(\"%s\", modifications);\n", dn, 0, 0);
    res = (*env)->CallIntMethod(env, java_back->backend, java_back->backendModify, conn->c_connid, dn, modifications);

    if (exceptionOccurred(env)) {

        Debug( LDAP_DEBUG_TRACE, "<== java_back_modify(): Failed modifying entry %s.\n", dn, 0, 0);

        rs->sr_err = LDAP_OPERATIONS_ERROR;
        send_ldap_result( op, rs );
        return LDAP_OPERATIONS_ERROR;
    }       

    rs->sr_err = res;
    send_ldap_result( op, rs );

    return 0;
}

