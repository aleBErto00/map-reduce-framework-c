#include <unistd.h> 
#include <sys/types.h>
#include <errno.h>

// Funzione per leggere esattamente n byte da un file descriptor (pipe)
ssize_t readn(int fd, void *buf, size_t n) { 
    size_t nleft = n;
    ssize_t nread;
    char *ptr = buf;

    while (nleft > 0) {
        if ((nread = read(fd, ptr, nleft)) < 0) {
            if (errno == EINTR) {
                nread = 0; // Chiamata interrotta da un segnale, riprova
            } else {
                return -1; // Errore reale
            }
        } else if (nread == 0) {
            break; // EOF (pipe chiusa dall'altro lato)
        }
        nleft -= (size_t)nread; 
        ptr += nread;
    }
    return (ssize_t)(n - nleft);
}

// Funzione per scrivere esattamente n byte su un file descriptor (pipe)
ssize_t writen(int fd, const void *buf, size_t n) {
    size_t nleft = n;
    ssize_t nwritten;
    const char *ptr = buf;

    while (nleft > 0) {
        if ((nwritten = write(fd, ptr, nleft)) <= 0) {
            if (nwritten < 0 && errno == EINTR) {
                nwritten = 0; // Scrittura interrotta da un segnale, riprova
            } else {
                return -1; // Errore irreversibile
            }
        } 
        nleft -= (size_t)nwritten;
        ptr += nwritten;
    }
    return (ssize_t)(n - nleft);
}