/*
 * mallocbomb.c: allocates as much memory as possible, touching every page.
 * Then copies stdin to stdout.  This is used to exercise out-of-memory code
 * paths in Marlin.
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int chunksize = 4 * 1024;

int
main(int argc, char *argv[])
{
	int total = 0;
	int *ptr;
	char buf[512];
	int rv;

	while ((ptr = malloc(chunksize)) != NULL) {
		*ptr = 0;
		total += chunksize;
	}

	(void) printf("malloc'd %d total bytes\n", total);

	while ((rv = read(STDIN_FILENO, buf, sizeof (buf))) != 0) {
		if (rv == -1) {
			perror("read");
			break;
		}

		(void) write(STDOUT_FILENO, buf, rv);
	}

	return (rv);
}
