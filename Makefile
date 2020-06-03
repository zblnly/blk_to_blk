stress:
	gcc -o aio-stress aio-stress.c
install:
	cp aio-stress /usr/bin
uninstall:
	rm -f /usr/bin/aio-stress
clean:
	rm -f aio-stress
