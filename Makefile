stress:
	gcc -o aio-stress -laio -lpthread aio-stress.c
install:
	cp aio-stress /usr/bin
uninstall:
	rm -f /usr/bin/aio-stress
clean:
	rm -f aio-stress
