all:
	gcc -o aio-stress -laio -lpthread aio-stress.c
	gcc -o b2b -laio -lpthread blk_to_blk.c
install:
	cp aio-stress /usr/bin
	cp b2b /usr/bin
uninstall:
	rm -f /usr/bin/aio-stress
	rm -f /usr/bin/b2b
clean:
	rm -f aio-stress
	rm -f b2b
