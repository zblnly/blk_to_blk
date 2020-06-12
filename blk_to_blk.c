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
#define _FILE_OFFSET_BITS 64
#define PROG_VERSION "0.01"
#define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64 

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
#include <sys/syscall.h> 

#define gettidv1() syscall(__NR_gettid)
#define DEBUG 0
#if DEBUG
#define PRINT(fmt, args...) \
do {								            \
	fprintf(stderr, "[B2B %d %17s %4d] " fmt, gettidv1(), __func__, __LINE__, ##args); \
} while (0)
#else
#define PRINT(fmt, args...) do {} while (0)
#endif

#define IO_FREE 0
#define IO_PENDING 1
#define IO_PENDING_WRITE 2
#define IO_PENDING_WRITE_SUBMIT 3
#define RUN_FOREVER -1

#ifndef O_DIRECT
#define O_DIRECT         040000 /* direct disk access hint */
#endif

enum {
    WRITE,
    READ,
    RWRITE,
    RREAD,
    LAST_STAGE,
};

#define USE_MALLOC 0
#define USE_SHM 1
#define USE_SHMFS 2

/* 
 * various globals, these are effectively read only by the time the threads
 * are started
 */
long stages = 0;
unsigned long page_size_mask;
int o_direct = O_DIRECT;
int o_sync = 0;
int latency_stats = 0;
int completion_latency_stats = 0;
int io_iter = 8;
int iterations = RUN_FOREVER;
int max_io_submit = 16;
long rec_len = 1024 * 1024;
int depth = 32;
int num_threads = 2;
int num_contexts = 1;
int fsync_stages = 1;
int use_shm = 0;
int shm_id;
char *unaligned_buffer = NULL;
char *aligned_buffer = NULL;
int padded_reclen = 0;
int unlink_files = 0;

struct io_unit;
struct thread_info;

/* pthread mutexes and other globals for keeping the threads in sync */
pthread_cond_t stage_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t stage_mutex = PTHREAD_MUTEX_INITIALIZER;
int threads_ending = 0;
int threads_starting = 0;
struct timeval global_stage_start_time;
struct thread_info *global_thread_info;

/* 
 * latencies during io_submit are measured, these are the 
 * granularities for deviations 
 */
#define DEVIATIONS 6
int deviations[DEVIATIONS] = { 100, 250, 500, 1000, 5000, 10000 };
struct io_latency {
    double max;
    double min;
    double total_io;
    double total_lat;
    double deviations[DEVIATIONS]; 
};

/* container for a series of operations to a file */
struct io_oper {
    /* already open file descriptor, valid for whatever operation you want */
    int fd;

    /* starting byte of the operation */
    off_t start;

    /* ending byte of the operation */
    off_t end;

    /* size of the read/write buffer */
    int reclen;

    /* max number of pending requests before a wait is triggered */
    int depth;

    /* last error, zero if there were none */
    int last_err;

    /* total number of errors hit. */
    int num_err;

    /* read,write, random, etc */
    int rw;

    /* number of ios that will get sent to aio */
    int total_ios;

    /* number of ios we've already sent */
    int started_ios;

    /* last offset used in an io operation */
    off_t last_offset;

    /* list management */
    struct io_oper *next;
    struct io_oper *prev;

    struct timeval start_time;

    char *file_name;
};

/* a single io, and all the tracking needed for it */
struct io_unit {
    /* note, iocb must go first! */
    struct iocb iocb;

    /* pointer to parent io operation struct */
    struct io_oper *io_oper;

    /* pointer to major thread */
    struct thread_info *major_thread;

    /* aligned buffer */
    char *buf;

    /* size of the aligned buffer (record size) */
    int buf_size;

    /* state of this io unit (free, pending, done) */
    int busy;

    /* result of last operation */
    long res;

    off_t offset;

    int len;

    struct io_unit *next;

    struct timeval io_start_time;       /* time of io_submit */
};

struct thread_info {
    io_context_t io_ctx;
    pthread_t tid;

    void *partner_thread;
    void *major_thread;
    pthread_mutex_t partner_mutex;

    /* allocated array of io_unit structs */
    struct io_unit *ios;

    /* list of io units available for io */
    struct io_unit *free_ious;

    /* list of io units available for write io */
    struct io_unit *ready_write_ious;

    /* number of io units in the ios array */
    int num_global_ios;

    /* number of io units in flight */
    int num_global_pending;

    /* preallocated array of iocb pointers, only used in run_active */
    struct iocb **iocbs;

    /* preallocated array of events */
    struct io_event *events;

    /* size of the events array */
    int num_global_events;

    /* latency stats for io_submit */
    struct io_latency io_submit_latency;

    /* list of operations still in progress, and of those finished */
    struct io_oper *active_opers;
    struct io_oper *finished_opers;

    /* how much io this thread did in the last stage */
    double stage_mb_trans;

    /* latency completion stats i/o time from io_submit until io_getevents */
    struct io_latency io_completion_latency;
};

/*
 * return seconds between start_tv and stop_tv in double precision
 */
static double time_since(struct timeval *start_tv, struct timeval *stop_tv)
{
    double sec, usec;
    double ret;
    sec = stop_tv->tv_sec - start_tv->tv_sec;
    usec = stop_tv->tv_usec - start_tv->tv_usec;
    if (sec > 0 && usec < 0) {
        sec--;
        usec += 1000000;
    } 
    ret = sec + usec / (double)1000000;
    if (ret < 0)
        ret = 0;
    return ret;
}

/*
 * return seconds between start_tv and now in double precision
 */
static double time_since_now(struct timeval *start_tv)
{
    struct timeval stop_time;
    gettimeofday(&stop_time, NULL);
    return time_since(start_tv, &stop_time);
}

/*
 * Add latency info to latency struct 
 */
static void calc_latency(struct timeval *start_tv, struct timeval *stop_tv,
            struct io_latency *lat)
{
    double delta;
    int i;
    delta = time_since(start_tv, stop_tv);
    delta = delta * 1000;

    if (delta > lat->max)
        lat->max = delta;
    if (!lat->min || delta < lat->min)
        lat->min = delta;
    lat->total_io++;
    lat->total_lat += delta;
    for (i = 0 ; i < DEVIATIONS ; i++) {
        if (delta < deviations[i]) {
        lat->deviations[i]++;
        break;
    }
    }
}

static void oper_list_add(struct io_oper *oper, struct io_oper **list)
{
    if (!*list) {
        *list = oper;
    oper->prev = oper->next = oper;
    return;
    }
    oper->prev = (*list)->prev;
    oper->next = *list;
    (*list)->prev->next = oper;
    (*list)->prev = oper;
    return;
}

static void oper_list_del(struct io_oper *oper, struct io_oper **list)
{
    if ((*list)->next == (*list)->prev && *list == (*list)->next) {
        *list = NULL;
    return;
    }
    oper->prev->next = oper->next;
    oper->next->prev = oper->prev;
    if (*list == oper)
        *list = oper->next;
}

char *stage_name(int rw) {
    switch(rw) {
    case WRITE:
        return "write";
    case READ:
        return "read";
    case RWRITE:
        return "random write";
    case RREAD:
        return "random read";
    }
    return "unknown";
}

static inline double oper_mb_trans(struct io_oper *oper) {
    return ((double)oper->started_ios * (double)oper->reclen) /
                (double)(1024 * 1024);
}
static void print_progress(struct io_unit *io) {
    double runtime;
	double tput;
	double mb;
    long progress;
    static int last_printtime = 0;
    struct io_oper *oper = io->io_oper;    
    int total_ios = oper->total_ios;

	runtime = time_since_now(&oper->start_time);
	mb = oper_mb_trans(oper);
	tput = mb / runtime;
    
    progress = (io->offset - oper->start)/rec_len;
    if ((!(progress%(total_ios/10))) && progress*100/total_ios || 
        ((int)runtime - last_printtime >=20)) {        
        fprintf(stderr, "%s on %s (%7.2f MB/s) %12.2f MB in %6.2fs -- percent %2d finish\n", 
            stage_name(oper->rw), oper->file_name, tput, mb, runtime, progress*100/total_ios);
        last_printtime = (int)runtime;
    }
}


/* worker func to check error fields in the io unit */
static int check_finished_io(struct io_unit *io) {
    int i;

    print_progress(io);
    
    if (io->res != io->buf_size) {

         struct stat s;
         fstat(io->io_oper->fd, &s);
  
         /*
          * If file size is large enough for the read, then this short
          * read is an error.
          */
         if ((io->io_oper->rw == READ) &&
             s.st_size > (io->iocb.u.c.offset + io->res)) {
  
                 fprintf(stderr, "io err %lu (%s) op %d, off %Lu size %d\n",
                         io->res, strerror(-io->res), io->iocb.aio_lio_opcode,
                         io->iocb.u.c.offset, io->buf_size);
                 io->io_oper->last_err = io->res;
                 io->io_oper->num_err++;
                 return -1;
         }
    }    
    return 0;
}

static void print_lat(char *str, struct io_latency *lat) {
    double avg = lat->total_lat / lat->total_io;
    int i;
    double total_counted = 0;
    fprintf(stderr, "%s min %.2f avg %.2f max %.2f\n\t", 
            str, lat->min, avg, lat->max);

    for (i = 0 ; i < DEVIATIONS ; i++) {
        fprintf(stderr, " %.0f < %d", lat->deviations[i], deviations[i]);
        total_counted += lat->deviations[i];
    }
    if (total_counted && lat->total_io - total_counted)
        fprintf(stderr, " < %.0f", lat->total_io - total_counted);
    fprintf(stderr, "\n");
    memset(lat, 0, sizeof(*lat));
}

static void print_latency(struct thread_info *t)
{
    struct io_latency *lat = &t->io_submit_latency;
    print_lat("latency", lat);
}

static void print_completion_latency(struct thread_info *t)
{
    struct io_latency *lat = &t->io_completion_latency;
    print_lat("completion latency", lat);
}

/*
 * updates the fields in the io operation struct that belongs to this
 * io unit, and make the io unit reusable again
 */
void finish_io(struct io_unit *io, long result, struct timeval *tv_now) {
    struct io_oper *oper = io->io_oper;
    struct thread_info *major_thread = io->major_thread;

    calc_latency(&io->io_start_time, tv_now, &major_thread->io_completion_latency);
    io->res = result;
    //PRINT("oper:%p io->busy:%d oper->rw:%d io->res:%lu oper->num_pending:%d\n", 
        //oper, io->busy, oper->rw, io->res, oper->num_pending);
    if (oper->rw == READ) {
        pthread_mutex_lock(&major_thread->partner_mutex);
        io->busy = IO_PENDING_WRITE;        
        io->next = major_thread->ready_write_ious;
        major_thread->ready_write_ious = io;        
        pthread_mutex_unlock(&major_thread->partner_mutex);
    } else if (oper->rw == WRITE) {        
        pthread_mutex_lock(&major_thread->partner_mutex);
        io->busy = IO_FREE;
        io->next = major_thread->free_ious;
        major_thread->free_ious = io;    
        check_finished_io(io);
        pthread_mutex_unlock(&major_thread->partner_mutex);
        
    } else {
        fprintf(stderr, "error io->io_oper->rw:%d\n", io->io_oper->rw);
        abort();
    }
}

int read_some_events(struct thread_info *t) {
    struct io_unit *event_io;
    struct io_event *event;
    int nr;
    int i; 
    int min_nr = io_iter;
    struct timeval stop_time;

    if (t->num_global_pending < io_iter)
        min_nr = t->num_global_pending;

    nr = io_getevents(t->io_ctx, min_nr, t->num_global_events, t->events,NULL);
    PRINT("id:%d min_nr:%d global:%2d nr:%d\n", 
        t - global_thread_info, min_nr, t->num_global_pending, nr);
    if (nr <= 0)
        return nr;

    gettimeofday(&stop_time, NULL);
    for (i = 0 ; i < nr ; i++) {
        event = t->events + i;
        event_io = (struct io_unit *)((unsigned long)event->obj); 
        finish_io(event_io, event->res, &stop_time);
        t->num_global_pending--;
    }
    return nr;
}

/* 
 * finds a free io unit, waiting for pending requests if required.  returns
 * null if none could be found
 */
static struct io_unit *find_iou(struct thread_info *t, struct io_oper *oper)
{
    struct io_unit *io = NULL;
    int nr;
    struct thread_info *major_thread = t->major_thread;
    struct thread_info *partner_thread = t->partner_thread;

retry:
    if (oper->rw == READ) {
        /* find free iou */
        pthread_mutex_lock(&t->partner_mutex);
        if (major_thread->free_ious) {
            io = major_thread->free_ious;            
            major_thread->free_ious = major_thread->free_ious->next;
            io->busy = IO_PENDING;
            io->major_thread = major_thread;
            io->res = 0;
            io->io_oper = oper;
            pthread_mutex_unlock(&t->partner_mutex);
            return io;
        }
        pthread_mutex_unlock(&t->partner_mutex);
#if 0
        PRINT("rw:%d partner_thread:%p t:%p read-global:%d write-global:%d free:%p ready:%p oper:%p\n", 
                oper->rw, partner_thread, t, t->num_global_pending, partner_thread->num_global_pending, 
                t->free_ious, major_thread->ready_write_ious, oper);
#endif
        if (t->num_global_pending) {
            nr = read_some_events(t);
            PRINT("rw:%d partner_thread:%p t:%p read-global:%d write-global:%d nr:%d free:%p\n", 
                oper->rw, partner_thread, t, partner_thread->num_global_pending, 
                major_thread->num_global_pending, nr, t->free_ious);
        }
        #if 0
        /* wait write finish */
        if (partner_thread->num_global_pending) {
            nr = read_some_events(partner_thread);
            PRINT("rw:%d partner_thread:%p t:%p read-global:%d write-global:%d nr:%d free:%p\n", 
                oper->rw, partner_thread, t, partner_thread->num_global_pending, 
                major_thread->num_global_pending, nr, t->free_ious);
            if (nr > 0)
                goto retry;   
        }
        #endif
        sched_yield();
        //usleep(500000);
        goto retry; 
    } else if (oper->rw == WRITE) {
        /* find ready write iou */
        pthread_mutex_lock(&major_thread->partner_mutex);
        if (major_thread->ready_write_ious) {
            io = major_thread->ready_write_ious;
            major_thread->ready_write_ious = major_thread->ready_write_ious->next;
            io->busy = IO_PENDING_WRITE;
            io->major_thread = major_thread;
            io->res = 0;
            io->io_oper = oper;
            pthread_mutex_unlock(&major_thread->partner_mutex);
            return io;
        }
        pthread_mutex_unlock(&major_thread->partner_mutex);
      #if 0  
        PRINT("rw:%d partner_thread:%p t:%p read-global:%d write-global:%d free:%p ready:%p rw:%d oper:%p\n", 
                oper->rw, partner_thread, t, major_thread->num_global_pending, t->num_global_pending, 
                major_thread->free_ious, major_thread->ready_write_ious, oper);
      #endif

        if (t->num_global_pending) {
            nr = read_some_events(t);
            PRINT("rw:%d partner_thread:%p t:%p read-global:%d write-global:%d nr:%d free:%p\n", 
                oper->rw, partner_thread, t, major_thread->num_global_pending, 
                partner_thread->num_global_pending, 
                nr, major_thread->free_ious);
        }    
        #if 0
        if (major_thread->num_global_pending) {
            nr = read_some_events(major_thread);
            PRINT("rw:%d partner_thread:%p t:%p read-global:%d write-global:%d nr:%d free:%p\n", 
                oper->rw, partner_thread, t, major_thread->num_global_pending, 
                partner_thread->num_global_pending, 
                nr, major_thread->free_ious);
            if (nr > 0)
                goto retry; 
        }
        #endif
        sched_yield();
        //usleep(500000);
        goto retry; 
    } else {
        fprintf(stderr, "error on oper->rw:%d\n", oper->rw);
        abort();
    }
    return NULL;
}

/*
 * wait for all pending requests for this io operation to finish
 */
static int io_oper_wait(struct thread_info *t, struct io_oper *oper) {
    struct io_event event;
    struct io_unit *event_io;
    struct thread_info *partner_thread = t->partner_thread;

    if (oper == NULL) {
        return 0;
    }

    if (oper->rw == READ) {        
        while (t->ready_write_ious) {
            PRINT("READ: t->ready_write_ious:%p write-global:%d\n", 
                t->ready_write_ious, partner_thread->num_global_pending);
            usleep(50000);  
        }
    } 
done:
    if (oper->num_err) {
        fprintf(stderr, "%u errors on oper, last %u\n", 
            oper->num_err, oper->last_err);
    }
    return 0;
}

off_t random_byte_offset(struct io_oper *oper) {
    off_t num;
    off_t rand_byte = oper->start;
    off_t range;
    off_t offset = 1;

    range = (oper->end - oper->start) / (1024 * 1024);
    if ((page_size_mask+1) > (1024 * 1024))
        offset = (page_size_mask+1) / (1024 * 1024);
    if (range < offset)
        range = 0;
    else
        range -= offset;

    /* find a random mb offset */
    num = 1 + (int)((double)range * rand() / (RAND_MAX + 1.0 ));
    rand_byte += num * 1024 * 1024;
    
    /* find a random byte offset */
    num = 1 + (int)((double)(1024 * 1024) * rand() / (RAND_MAX + 1.0));

    /* page align */
    num = (num + page_size_mask) & ~page_size_mask;
    rand_byte += num;

    if (rand_byte + oper->reclen > oper->end) {
    rand_byte -= oper->reclen;
    }
    return rand_byte;
}

/* 
 * build an aio iocb for an operation, based on oper->rw and the
 * last offset used.  This finds the struct io_unit that will be attached
 * to the iocb, and things are ready for submission to aio after this
 * is called.
 *
 * returns null on error
 */
static struct io_unit *build_iocb(struct thread_info *t, struct io_oper *oper)
{
    struct io_unit *io;

    io = find_iou(t, oper);
    if (!io) {
        //fprintf(stderr, "unable to find io unit\n");
        return NULL;
    }
    
    if (io->busy == IO_PENDING_WRITE) {
        io_prep_pwrite(&io->iocb, oper->fd, io->buf, io->len, io->offset);        
        //PRINT("write: io->offset %llu\n", io->offset);
    } else if (io->busy == IO_PENDING) {
        io->offset = oper->last_offset;
        if (io->offset + oper->reclen > oper->end)
            io->len = oper->end - io->offset;   
        else
            io->len = oper->reclen;
        io_prep_pread(&io->iocb, oper->fd, io->buf, io->len, io->offset);
        oper->last_offset += io->len;
        //PRINT("read: io->offset %llu\n", io->offset);
    }
    return io;
}

/* 
 * wait for any pending requests, and then free all ram associated with
 * an operation.  returns the last error the operation hit (zero means none)
 */
static int
finish_oper(struct thread_info *t, struct io_oper *oper)
{
    unsigned long last_err;

    io_oper_wait(t, oper);
    last_err = oper->last_err;
    if (t->num_global_pending > 0) {
        fprintf(stderr, "thread num_global_pending is %d\n", t->num_global_pending);
    }
    close(oper->fd);
    free(oper);
    return last_err;
}

/* 
 * allocates an io operation and fills in all the fields.  returns
 * null on error
 */
static struct io_oper * 
create_oper(int fd, int rw, off_t start, off_t end, int reclen, int depth,
            int iter, char *file_name)
{
    struct io_oper *oper;

    oper = malloc (sizeof(*oper));
    if (!oper) {
        fprintf(stderr, "unable to allocate io oper\n");
        return NULL;
    }
    memset(oper, 0, sizeof(*oper));

    oper->depth = depth;
    oper->start = start;
    oper->end = end;
    oper->last_offset = oper->start;
    oper->fd = fd;
    oper->reclen = reclen;
    oper->rw = rw;
    oper->total_ios = (oper->end - oper->start + oper->reclen - 1) / oper->reclen;
    oper->file_name = file_name;
    PRINT("oper->total_ios:%d reclen:%d start:%llu end:%llu\n", 
        oper->total_ios, oper->reclen, oper->start, oper->end);

    return oper;
}

/*
 * does setup on num_ios worth of iocbs, but does not actually
 * start any io
 */
int build_oper(struct thread_info *t, struct io_oper *oper, int num_ios, 
               struct iocb **my_iocbs) 
{
    int i;
    struct io_unit *io;

    if (oper->started_ios == 0)
        gettimeofday(&oper->start_time, NULL);
    
    //PRINT("num_ios %d\n", num_ios);
    
    for( i = 0 ; i < num_ios ; i++) {
        io = build_iocb(t, oper);
        if (!io) 
                return i;
        my_iocbs[i] = &io->iocb;
    }
    return num_ios;
}

/*
 * runs through the iocbs in the array provided and updates
 * counters in the associated oper struct
 */
static void update_iou_counters(struct iocb **my_iocbs, int nr,
    struct timeval *tv_now) 
{
    struct io_unit *io;
    int i;
    for (i = 0 ; i < nr ; i++) {
        io = (struct io_unit *)(my_iocbs[i]);   
        io->io_oper->started_ios++;
        io->io_start_time = *tv_now;    /* set time of io_submit */
    }
}

/* starts some io for a given file, returns zero if all went well */
int run_built(struct thread_info *t, int num_ios, struct iocb **my_iocbs) 
{
    int ret;
    struct timeval start_time;
    struct timeval stop_time;

resubmit:
    gettimeofday(&start_time, NULL);
    ret = io_submit(t->io_ctx, num_ios, my_iocbs);
    gettimeofday(&stop_time, NULL);
    calc_latency(&start_time, &stop_time, &t->io_submit_latency);

    if (ret != num_ios) {
        PRINT("ret:%d != num_ios:%d\n", ret, num_ios);
        /* some ios got through */
        if (ret > 0) {
            update_iou_counters(my_iocbs, ret, &stop_time);
            t->num_global_pending += ret;
            my_iocbs += ret;
            num_ios -= ret;
        }
        /* 
         * we've used all the requests allocated in aio_init, wait and
         * retry
         */
        if (ret > 0 || ret == -EAGAIN) {
            int old_ret = ret;             
            PRINT("will read_some_events\n");
            if ((ret = read_some_events(t) > 0)) {
                goto resubmit;
            } else {
                fprintf(stderr, "ret was %d and now is %d\n", ret, old_ret);
                abort();
            }
        }

        fprintf(stderr, "ret %d (%s) on io_submit\n", ret, strerror(-ret));
        return -1;
    }
    update_iou_counters(my_iocbs, ret, &stop_time);
    t->num_global_pending += ret;
    return 0;
}
/*
 * runs through all the io operations on the active list, and starts
 * a chunk of io on each.  If any io operations are completely finished,
 * it either switches them to the next stage or puts them on the 
 * finished list.
 *
 * this function stops after max_io_submit iocbs are sent down the 
 * pipe, even if it has not yet touched all the operations on the 
 * active list.  Any operations that have finished are moved onto
 * the finished_opers list.
 */
static int run_active_list(struct thread_info *t,
             int io_iter,
             int max_io_submit)
{
    struct io_oper *oper;
    struct io_oper *built_opers = NULL;    
    struct iocb **my_iocbs = t->iocbs;
    int ret = 0;
    int num_built = 0;
    int nums_io = io_iter;
   
    oper = t->active_opers;
    while(oper) { 

        PRINT("rw:%d id:%d blt:%d oper:%p total:%d started:%d global:%2d ret:%d\n", 
           oper->rw, t - global_thread_info, num_built, oper, oper->total_ios, 
           oper->started_ios, t->num_global_pending, ret);  
 
        if (!t->num_global_pending && (oper->started_ios == oper->total_ios)) {            
            oper_list_del(oper, &t->active_opers);
            oper_list_add(oper, &t->finished_opers);
            PRINT("rw:%d finish id:%d blt:%d oper:%p total:%d started:%d global:%2d ret:%d\n", 
                oper->rw, t - global_thread_info, num_built, oper, oper->total_ios,  
                oper->started_ios, t->num_global_pending, ret);
            break;
        }

        if (t->num_global_pending && (oper->started_ios == oper->total_ios)) {
            read_some_events(t);
            continue;
        }

        if ((oper->total_ios - oper->started_ios) < io_iter)
            nums_io = (oper->total_ios - oper->started_ios);
        
        ret = build_oper(t, oper, nums_io, my_iocbs);
        PRINT("rw:%d id:%d blt:%d oper:%p total:%d started:%d global:%2d ret:%d\n", 
           oper->rw, t - global_thread_info, num_built, oper, oper->total_ios, oper->started_ios, 
           t->num_global_pending, ret);  
        if (ret > 0) {
            my_iocbs += ret;
            num_built += ret;
            oper_list_del(oper, &t->active_opers);
            oper_list_add(oper, &built_opers);
            oper = t->active_opers;
        }   
    }
    if (num_built) {
        ret = run_built(t, num_built, t->iocbs);
        if (ret < 0) {
            fprintf(stderr, "error %d on run_built\n", ret);
            exit(1);
        }
        while(built_opers) {
            oper = built_opers;
            oper_list_del(oper, &built_opers);
            oper_list_add(oper, &t->active_opers);
        }
    }
    
    return 0;
}

void drop_shm() {
    int ret;
    struct shmid_ds ds;
    if (use_shm != USE_SHM)
        return;

    ret = shmctl(shm_id, IPC_RMID, &ds);
    if (ret) {
        perror("shmctl IPC_RMID");
    }
}

void aio_setup(io_context_t *io_ctx, int n)
{
    int res = io_queue_init(n, io_ctx);
    if (res != 0) {
    fprintf(stderr, "io_queue_setup(%d) returned %d (%s)\n",
        n, res, strerror(-res));
    exit(3);
    }
}

/*
 * allocate io operation and event arrays for a given thread
 */
int setup_ious(struct thread_info *t, int depth, 
          int reclen, int max_io_submit) {
    int i;
    size_t bytes = depth * sizeof(*t->ios);
    struct thread_info *major_thread = t->major_thread;   

    t->ios = malloc(bytes);
    if (!t->ios) {
        fprintf(stderr, "unable to allocate io units\n");
        return -1;
    }
    memset(t->ios, 0, bytes);

    for (i = 0 ; i < depth; i++) {
        t->ios[i].buf = aligned_buffer;
        aligned_buffer += padded_reclen;
        t->ios[i].buf_size = reclen;
        memset(t->ios[i].buf, 0, reclen);
        t->ios[i].next = major_thread->free_ious;
        major_thread->free_ious = t->ios + i;
    }

    t->iocbs = malloc(sizeof(struct iocb *) * max_io_submit);
    if (!t->iocbs) {
        fprintf(stderr, "unable to allocate iocbs\n");
        goto free_buffers;
    }

    memset(t->iocbs, 0, max_io_submit * sizeof(struct iocb *));

    t->events = malloc(sizeof(struct io_event) * depth);
    if (!t->events) {
        fprintf(stderr, "unable to allocate ram for events\n");
        goto free_buffers;
    }
    memset(t->events, 0, sizeof(struct io_event)*depth);

    t->num_global_ios = depth;
    t->num_global_events = t->num_global_ios;
    return 0;

free_buffers:
    if (t->ios)
        free(t->ios);
    if (t->iocbs)
        free(t->iocbs);  
    if (t->events)
        free(t->events);
    return -1;
}

/*
 * The buffers used for file data are allocated as a single big
 * malloc, and then each thread and operation takes a piece and uses
 * that for file data.  This lets us do a large shm or bigpages alloc
 * and without trying to find a special place in each thread to map the
 * buffers to
 */
int setup_shared_mem(int num_threads, int num_files, int depth, 
                     int reclen, int max_io_submit) 
{
    char *p = NULL;
    size_t total_ram;
    
    padded_reclen = (reclen + page_size_mask) / (page_size_mask+1);
    padded_reclen = padded_reclen * (page_size_mask+1);
    total_ram = num_files * depth * padded_reclen;

    if (use_shm == USE_MALLOC) {
        p = malloc(total_ram + page_size_mask);
    } else if (use_shm == USE_SHM) {
        shm_id = shmget(IPC_PRIVATE, total_ram, IPC_CREAT | 0700);
        if (shm_id < 0) {
            perror("shmget");
            drop_shm();
            goto free_buffers;
        }
        p = shmat(shm_id, (char *)0x50000000, 0);
            if ((long)p == -1) {
            perror("shmat");
            goto free_buffers;
        }
        /* won't really be dropped until we shmdt */
        drop_shm();
    } else if (use_shm == USE_SHMFS) {
        char mmap_name[16]; /* /dev/shm/ + null + XXXXXX */    
        int fd;

        strcpy(mmap_name, "/dev/shm/XXXXXX");
        fd = mkstemp(mmap_name);
            if (fd < 0) {
            perror("mkstemp");
            goto free_buffers;
        }
        unlink(mmap_name);
        ftruncate(fd, total_ram);
        shm_id = fd;
        p = mmap((char *)0x50000000, total_ram,
                 PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

        if (p == MAP_FAILED) {
            perror("mmap");
            goto free_buffers;
        }
    }
    if (!p) {
        fprintf(stderr, "unable to allocate buffers\n");
        goto free_buffers;
    }
    unaligned_buffer = p;
    p = (char*)((intptr_t) (p + page_size_mask) & ~page_size_mask);
    aligned_buffer = p;
    return 0;

free_buffers:
    drop_shm();
    if (unaligned_buffer)
        free(unaligned_buffer);
    return -1;
}

/* this is the meat of the state machine.  There is a list of
 * active operations structs, and as each one finishes the required
 * io it is moved to a list of finished operations.  Once they have
 * all finished whatever stage they were in, they are given the chance
 * to restart and pick a different stage (read/write/random read etc)
 *
 * various timings are printed in between the stages, along with
 * thread synchronization if there are more than one threads.
 */
int worker(struct thread_info *t)
{
    struct io_oper *oper;
    struct timeval stage_time;
    int status = 0;
    int cnt;
    int thread_id = 0;
    aio_setup(&t->io_ctx, 512);

    thread_id = t - global_thread_info;

restart:
    if (num_threads > 1) {
        pthread_mutex_lock(&stage_mutex);
        threads_starting++;
        if (threads_starting == num_threads) {
            threads_ending = 0;
            gettimeofday(&global_stage_start_time, NULL);
            pthread_cond_broadcast(&stage_cond);
        }
        while (threads_starting != num_threads)
            pthread_cond_wait(&stage_cond, &stage_mutex);
        pthread_mutex_unlock(&stage_mutex);
    }
    if (t->active_opers) {
        gettimeofday(&stage_time, NULL);
        t->stage_mb_trans = 0;
    }

    cnt = 0;
    /* first we send everything through aio */
    while (t->active_opers) {        
        if (cnt >= iterations && iterations != RUN_FOREVER) {
			oper = t->active_opers;
			oper_list_del(oper, &t->active_opers);
			oper_list_add(oper, &t->finished_opers);
		} else {
			run_active_list(t, io_iter, max_io_submit);
		}
        cnt++;
    }
    
    PRINT("finish thread_id:%d global:%2d\n", thread_id, t->num_global_pending);
    if (latency_stats)
        print_latency(t);

    if (completion_latency_stats)
        print_completion_latency(t);

    /* then we wait for all the operations to finish */
    oper = t->finished_opers;
    do {
        if (!oper)
            break;
        PRINT("wait: id:%d total:%d started:%d global:%2d\n", 
            thread_id, oper->total_ios, oper->started_ios, 
            t->num_global_pending);
        io_oper_wait(t, oper); 
        PRINT("wait: id:%d total:%d started:%d global:%2d\n", 
            thread_id, oper->total_ios, oper->started_ios, 
            t->num_global_pending);
        oper = oper->next;
    } while(oper != t->finished_opers);

    /* then we do an fsync to get the timing for any future operations
     * right, and check to see if any of these need to get restarted
     */
    oper = t->finished_opers;
    while(oper) {
        if (fsync_stages) {
            fsync(oper->fd);            
        }
        t->stage_mb_trans += oper_mb_trans(oper);        
        oper = oper->next;
        if (oper == t->finished_opers)
            break;
        PRINT("thread %d totals %.2f MB\n", thread_id, t->stage_mb_trans);
    } 

    if (t->stage_mb_trans) {
        double seconds = time_since_now(&stage_time);
        fprintf(stderr, "%s thread %d totals (%.2f MB/s) %.2f MB in %.2fs\n", 
            thread_id%2 ? "write":"read ", thread_id, t->stage_mb_trans/seconds, 
            t->stage_mb_trans, seconds);
    }
    
    PRINT("thread %d finish cnt:%d\n", thread_id, cnt);
    if (num_threads > 1) {
        pthread_mutex_lock(&stage_mutex);
        threads_ending++;
        if (threads_ending == num_threads) {
            threads_starting = 0;
            pthread_cond_broadcast(&stage_cond);        
        }

        PRINT("thread %d finish threads_ending:%d num_threads:%d\n", 
            thread_id, threads_ending, num_threads);
        while(threads_ending != num_threads)
            pthread_cond_wait(&stage_cond, &stage_mutex);
        PRINT("thread %d finish threads_ending:%d num_threads:%d\n", 
            thread_id, threads_ending, num_threads);
        pthread_mutex_unlock(&stage_mutex);
    }
    
    /* finally, free all the ram */
    while(t->finished_opers) {
        oper = t->finished_opers;
        oper_list_del(oper, &t->finished_opers);
        status = finish_oper(t, oper);
        PRINT("thread %d finish threads_ending:%d threads_ending%d\n", thread_id, threads_ending);
    }

    if (t->num_global_pending) {
        fprintf(stderr, "global num pending is %d\n", t->num_global_pending);
    }
    io_queue_release(t->io_ctx);
    
    return status;
}

typedef void * (*start_routine)(void *);
int run_workers(struct thread_info *t, int num_threads)
{
    int ret;
    int thread_ret;
    int i;

    for(i = 0 ; i < num_threads ; i++) {
        ret = pthread_create(&t[i].tid, NULL, (start_routine)worker, t + i);
        if (ret) {
            perror("pthread_create");
            exit(1);
        }
    }
    for(i = 0 ; i < num_threads ; i++) {
        ret = pthread_join(t[i].tid, (void *)&thread_ret);
        if (ret) {
            perror("pthread_join");
            exit(1);
        }
    }
    return 0;
}

off_t parse_size(char *size_arg, off_t mult) {
    char c;
    int num;
    off_t ret;
    c = size_arg[strlen(size_arg) - 1];
    if (c > '9') {
        size_arg[strlen(size_arg) - 1] = '\0';
    }
    num = atoi(size_arg);
    switch(c) {
    case 'g':
    case 'G':
        mult = 1024 * 1024 * 1024;
    break;
    case 'm':
    case 'M':
        mult = 1024 * 1024;
    break;
    case 'k':
    case 'K':
        mult = 1024;
    break;
    case 'b':
    case 'B':
        mult = 1;
    break;
    }
    ret = mult * num;
    return ret;
}

void print_usage(void) {
    printf("usage: b2b [-r size] [-a size] [-d num] [-i num]\n");
    printf("           [-c num] [-O 0|1][-nhS ] file1 file2\n");
    printf("\t-a size in KB at which to align buffers\n");
    printf("\t-c number of io contexts per file\n");
    printf("\t-r record size in KB used for each io, default 1MB\n");
    printf("\t-d number of pending aio requests for each file, default 32\n");
    printf("\t-i number of ios per file sent per round, default 8\n");
    printf("\t-I total number of ayncs IOs the program will run, default is run until finish\n");
    printf("\t-O 1, use O_DIRECT, 0, use BUFFER IO\n");
    printf("\t-S Use O_SYNC for writes\n");
    printf("\t-m shm use ipc shared memory for io buffers instead of malloc\n");
    printf("\t-m shmfs mmap a file in /dev/shm for io buffers\n");
    printf("\t-n no fsyncs before exit\n");
    printf("\t-l print io_submit latencies after each stage\n");
    printf("\t-L print io completion latencies after each stage\n");
    printf("\t-h this message\n");
    printf("\n\t   the size options (-a and -r) allow modifiers -s 400{k,m,g}\n");
    printf("\t   translate to 400KB, 400MB and 400GB\n");
    printf("version %s\n", PROG_VERSION);
}

off_t get_file_size(int fd)
{
    int ret = 0;
    off_t filesize = 0;
    struct stat s;
    ret = fstat(fd, &s);
    if (ret < 0) {        
        return 0;
    }
    if (S_ISBLK(s.st_mode)) {
        filesize = lseek64(fd, 0, SEEK_END);
        if (-1 == filesize) {
            fprintf(stderr,
                "Error while opening %llu : %s\n",
                filesize, strerror(errno));
            return 0;
        }     
    } else if (S_ISREG(s.st_mode)) {
        filesize = s.st_size;
    }
    return filesize;
}

int main(int ac, char **av) 
{
    int rwfd;
    int rwfd1;
    int i;
    int j;
    int c;
    off_t file_size;
    struct io_oper *oper;
    struct io_oper *oper1;
    int status = 0;
    int num_files = 0;  
    struct thread_info *t;
    page_size_mask = getpagesize() - 1;

    while(1) {
        c = getopt(ac, av, "a:c:m:r:d:i:I:O:lLnhS");
        if  (c < 0)
            break;

        switch(c) {
        case 'a':
            page_size_mask = parse_size(optarg, 1024);
            page_size_mask--;
            break;
        case 'c':
            num_contexts = atoi(optarg);
            break;
        case 'd':
            depth = atoi(optarg);
            break;
        case 'r':
            rec_len = parse_size(optarg, 1024);
            break;
        case 'i':
            io_iter = atoi(optarg);
            break;
        case 'I':
            iterations = atoi(optarg);
            break;
        case 'n':
            fsync_stages = 0;
            break;
        case 'l':
            latency_stats = 1;
            break;
        case 'L':
            completion_latency_stats = 1;
            break;
        case 'm':
            if (!strcmp(optarg, "shm")) {
            fprintf(stderr, "using ipc shm\n");
                use_shm = USE_SHM;
            } else if (!strcmp(optarg, "shmfs")) {
                fprintf(stderr, "using /dev/shm for buffers\n");
            use_shm = USE_SHMFS;
            }
            break;
        case 'O':
            i = atoi(optarg);
            if (!i)
                o_direct = 0;
            else
                o_direct = O_DIRECT;
            break;
        case 'S':
            o_sync = O_SYNC;
            break;
        case 'h':
        default:
            print_usage();
            exit(1);
        }
    }

    /* 
     * make sure we don't try to submit more ios than we have allocated
     * memory for
     */
    if (depth < io_iter) {
        io_iter = depth;
        fprintf(stderr, "dropping io_iter to %d\n", io_iter);
    }

    if (ac - optind != 2) {
        print_usage();
        exit(1);
    }
    num_files = 2;
    
    if (num_threads < num_files * num_contexts) {
        num_threads = num_files * num_contexts;
        fprintf(stderr, "increasing thread count to the number of contexts %d\n", 
                num_threads);
    }
    
    t = malloc(num_threads * sizeof(*t));
    if (!t) {
        perror("malloc");
        exit(1);
    }
    global_thread_info = t;   
    max_io_submit = depth;
    
    stages = READ;    

    fprintf(stderr, "record size %luKB, depth %d, ios per iteration %d\n", rec_len / 1024, depth, io_iter);
    fprintf(stderr, "max io_submit %d, buffer alignment set to %luKB\n", 
            max_io_submit, (page_size_mask + 1)/1024);
    fprintf(stderr, "threads %d files %d contexts %d\n", 
            num_threads, num_files, num_contexts);
    /* open all the files and do any required setup for them */
    for (i = optind ; i < ac ; i = i + 2) {
        for (j = 0 ; j < num_contexts ; j++) {
            off_t delta = 0;
            off_t start = 0;
            off_t filesize0 = 0;
            off_t filesize1 = 0;
            off_t end = 0;
            rwfd = open(av[i], O_CREAT | O_RDWR | O_LARGEFILE | o_direct | o_sync , 0600);
            assert(rwfd != -1);

            rwfd1 = open(av[i+1], O_CREAT | O_RDWR | O_LARGEFILE | o_direct | o_sync, 0600);
            assert(rwfd1 != -1);

            filesize0 = get_file_size(rwfd);
            filesize1 = get_file_size(rwfd1);    
            if (!filesize0 || !filesize1) {
                fprintf(stderr, "filesize0 or filesize1 is 0!\n");
                exit(-1);
            }
       
            if (filesize0 > filesize1)
                file_size = filesize1;
            else
                file_size = filesize0;
           
            delta = (file_size)/(rec_len*num_contexts);
            delta = delta*rec_len;

            start = j * delta;
            end = start + delta;
            if (j == num_contexts - 1)
                end = file_size;
            
            fprintf(stderr, "start %llu, end %llu, file_size %LuMB, %s size %LuMB, %s size %LuMB\n", 
                start, end, file_size/(1024*1024),
                av[i], filesize0/(1024*1024), 
                av[i+1], filesize1/(1024*1024));
            
            oper = create_oper(rwfd, READ, start, end, rec_len, 
                       depth, io_iter, av[i]);
            if (!oper) {
                fprintf(stderr, "error in create_oper\n");
                exit(-1);
            }
            oper_list_add(oper, &t[2*j].active_opers);
            t[2*j].partner_thread = &t[2*i+1];
            pthread_mutex_init(&t[2*j].partner_mutex, NULL);
            t[2*j].major_thread = &t[2*j];
 
            oper1 = create_oper(rwfd1, WRITE, start, end, rec_len, 
                       depth, io_iter, av[i+1]);
            if (!oper1) {
                fprintf(stderr, "error in create_oper1\n");
                exit(-1);
            }
            oper_list_add(oper1, &t[2*j+1].active_opers);  
            t[2*j+1].partner_thread = &t[2*j];
            pthread_mutex_init(&t[j].partner_mutex, NULL);
            t[2*j+1].major_thread = &t[2*j];
            PRINT("%d: major_thread:%p partner_thread:%p %d: major_thread:%p partner_thread:%p\n", 
                2*j, t[2*j].major_thread, t[2*j].partner_thread, 
                2*j+1, t[2*j+1].major_thread, t[2*j+1].partner_thread);
        }
    }
    if (setup_shared_mem(num_threads, num_files * num_contexts, 
                         depth, rec_len, max_io_submit)) {
        exit(1);
    }
    for (i = 0 ; i < num_threads ; i++) {
        if (setup_ious(&t[i], depth, rec_len, max_io_submit))
            exit(1);
    }
    if (num_threads > 1) {
        printf("Running multi thread version num_threads:%d\n", num_threads);
        run_workers(t, num_threads);
    } else {
        printf("Running single thread version\n");
        status = worker(t);
    }

    if (status) {
        exit(1);
    }
    return status;
}

