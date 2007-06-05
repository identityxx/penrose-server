#include <stdio.h>
#include <netdb.h>

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("Usage: test-netgroup <netgroup name>\n");
        return -1;
    }

    char* netgroup = argv[1];

    char* host;
    char* user;
    char* domain;

    setnetgrent(netgroup);

    int more = getnetgrent(&host, &user, &domain);

    while (more > 0) {

        printf("Host   : %s\n", host);
        printf("User   : %s\n", user);
        printf("Domain : %s\n", domain);
        printf("\n");

        more = getnetgrent(&host, &user, &domain);
    } 

    endnetgrent();

    return 0;
}
