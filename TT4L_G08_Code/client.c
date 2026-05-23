// client.c - Interactive Game Client
// Build: gcc -O2 -Wall -Wextra client.c -o client
// Run:   ./client 127.0.0.1 5555

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>
#include <errno.h>

// ============================================================
// MAIN FUNCTION
// ============================================================

int main(int argc, char **argv) {
    // === Check command-line arguments ===
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <ip> <port>\n", argv[0]);
        fprintf(stderr, "Example: %s 127.0.0.1 5555\n", argv[0]);
        return 1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { 
        perror("socket"); 
        return 1; 
    }

    // === Setup server address ===
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;                    
    addr.sin_port = htons(atoi(argv[2]));        
    inet_pton(AF_INET, argv[1], &addr.sin_addr);

    // === Connect to server ===
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        perror("connect");
        return 1;
    }

    fd_set rfds;
    char in[256], buf[512];

    // === Main I/O loop ===
    while (1) {
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);  // Monitor keyboard input
        FD_SET(fd, &rfds);             // Monitor server messages

        int maxfd = fd > STDIN_FILENO ? fd : STDIN_FILENO;

        // Wait for input from either keyboard or server
        if (select(maxfd + 1, &rfds, NULL, NULL, NULL) < 0) {
            if (errno == EINTR) continue;  // Interrupted by signal
            perror("select");
            break;
        }

        // === Handle server message ===
        if (FD_ISSET(fd, &rfds)) {
            ssize_t n = recv(fd, buf, sizeof(buf)-1, 0);
            if (n <= 0) {
                printf("\nServer closed connection.\n");
                break;
            }
            buf[n] = 0;  // Null-terminate
            printf("%s", buf);  // Print server message
            fflush(stdout);
        }

        // === Handle keyboard input ===
        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            if (!fgets(in, sizeof(in), stdin)) break;  // Ctrl+D detected
            send(fd, in, strlen(in), 0);  // Send command to server
        }
    }

    close(fd);
    return 0;
}