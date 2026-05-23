CC=gcc
CFLAGS=-O2 -Wall -Wextra
LDFLAGS=-pthread

all: server client

server: server.c
	$(CC) $(CFLAGS) server.c -o server $(LDFLAGS)

client: client.c
	$(CC) $(CFLAGS) client.c -o client

clean:
	rm -f server client 