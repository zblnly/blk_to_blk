all:
	gcc -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -o aio-stress -laio -lpthread aio-stress.c
	gcc -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -o b2b -laio -lpthread blk_to_blk.c
install:
	cp aio-stress /usr/bin
	cp b2b /usr/bin
uninstall:
	rm -f /usr/bin/aio-stress
	rm -f /usr/bin/b2b
clean:
	rm -f aio-stress
	rm -f b2b
