# blk_to_blk
linux block device to block device copy

[root@52_204 blk_to_blk]# ./b2b
usage: b2b [-r size] [-a size] [-d num] [-i num]
           [-c num] [-O 0|1][-nhS ] file1 file2
        -a size in KB at which to align buffers
        -c number of io contexts per file
        -r record size in KB used for each io, default 1MB
        -d number of pending aio requests for each file, default 32
        -i number of ios per file sent per round, default 8
        -I total number of ayncs IOs the program will run, default is run until finish
        -O 1, use O_DIRECT, 0, use BUFFER IO
        -S Use O_SYNC for writes
        -m shm use ipc shared memory for io buffers instead of malloc
        -m shmfs mmap a file in /dev/shm for io buffers
        -n no fsyncs before exit
        -l print io_submit latencies after each stage
        -L print io completion latencies after each stage
        -h this message

           the size options (-a and -r) allow modifiers -s 400{k,m,g}
           translate to 400KB, 400MB and 400GB
version 1.0.0
[root@52_204 blk_to_blk]# 



