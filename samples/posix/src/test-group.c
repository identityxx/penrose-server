#include <stdio.h>
#include <sys/types.h>
#include <grp.h>

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("Usage: test-group <name>\n");
        return -1;
    }

    char* name = argv[1];

    struct group* result = getgrnam(name);

    if (!result) {
        printf("Group %s not found.\n", name);
        return -1;
    }

    printf("Name      : %s\n", result->gr_name);
    printf("Password  : %s\n", result->gr_passwd);
    printf("GID       : %d\n", result->gr_gid);

    char** member = result->gr_mem;
    while (member != NULL && (*member) != NULL) {
        printf("Member    : %s\n", (*member));     
        member++;
    }

    printf("\n");

    return 0;

}
