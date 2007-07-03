#include <stdio.h>
#include <sys/types.h>
#include <pwd.h>

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("Usage: test-user <name>\n");
        return -1;
    }

    char* name = argv[1];

    struct passwd* result = getpwnam(name);

    if (!result) {
        printf("User %s not found.\n", name);
        return -1;
    }

    printf("Name      : %s\n", result->pw_name);
    printf("Password  : %s\n", result->pw_passwd);
    printf("UID       : %d\n", result->pw_uid);
    printf("GID       : %d\n", result->pw_gid);
    printf("Gecos     : %s\n", result->pw_gecos);
    printf("Directory : %s\n", result->pw_dir);
    printf("Shell     : %s\n", result->pw_shell);
    printf("\n");

    return 0;
}
