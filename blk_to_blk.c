/*
 * Copyright (c) 2004 SuSE, Inc.  All Rights Reserved.
 * 
 *
 * b2b 
 *
 * intends to do block device to block devcie copy
 *
 *
 * compile with gcc -Wall -laio -lpthread -o b2b blk_to_blk.c
 *
 * run b2b -h to see the options
 *
 * Please mail BinZhang (zblnly@qq.com) with bug reports or patches
 */
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <libaio.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <string.h>
#include <pthread.h>

#define PROG_VERSION "0.01"
void print_usage(void) {
    printf("usage: aio-stress [-s size] [-r size] [-a size] [-d num] [-b num]\n");
    printf("                  [-i num] [-t num] [-c num] [-C size] [-nxhOS ]\n");
    printf("                  file1 [file2 ...]\n");
    printf("\t-a size in KB at which to align buffers\n");
    printf("\t-b max number of iocbs to give io_submit at once\n");
    printf("\t-c number of io contexts per file\n");
    printf("\t-C offset between contexts, default 2MB\n");
    printf("\t-s size in MB of the test file(s), default 1024MB\n");
    printf("\t-r record size in KB used for each io, default 64KB\n");
    printf("\t-d number of pending aio requests for each file, default 64\n");
    printf("\t-i number of ios per file sent before switching\n\t   to the next file, default 8\n");
    printf("\t-I total number of ayncs IOs the program will run, default is run until Cntl-C\n");
    printf("\t-O Use O_DIRECT (not available in 2.4 kernels),\n");
    printf("\t-S Use O_SYNC for writes\n");
    printf("\t-o add an operation to the list: write=0, read=1,\n"); 
    printf("\t   random write=2, random read=3.\n");
    printf("\t   repeat -o to specify multiple ops: -o 0 -o 1 etc.\n");
    printf("\t-m shm use ipc shared memory for io buffers instead of malloc\n");
    printf("\t-m shmfs mmap a file in /dev/shm for io buffers\n");
    printf("\t-n no fsyncs between write stage and read stage\n");
    printf("\t-l print io_submit latencies after each stage\n");
    printf("\t-L print io completion latencies after each stage\n");
    printf("\t-t number of threads to run\n");
    printf("\t-u unlink files after completion\n");
    printf("\t-v verification of bytes written\n");
    printf("\t-x turn off thread stonewalling\n");
    printf("\t-h this message\n");
    printf("\n\t   the size options (-a -s and -r) allow modifiers -s 400{k,m,g}\n");
    printf("\t   translate to 400KB, 400MB and 400GB\n");
    printf("version %s\n", PROG_VERSION);
}
	
 int main(int ac, char **av) 
{
    int c;
    int status = 0;

    while(1) {
	c = getopt(ac, av, "a:b:c:C:m:s:r:d:i:I:o:t:lLnhOSxvu");
	if  (c < 0)
	    break;

        switch(c) {
	default:
	    print_usage();
	    exit(1);
	}
    }

    if (status) {
	exit(1);
    }
    return status;
}