#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Utility per stampare in forma leggibile il contenuto di un file di output .mro.
// Il formato binario è: [tlen:int][token:tlen byte][rlen:int][result:rlen byte]
// Se il risultato è 4 byte lo interpreta come int, se è 8 byte come long,
// altrimenti stampa solo la dimensione in byte.

// Funzione principale che legge un file di output .mro e stampa il contenuto in forma leggibile
int main(int argc, char **argv) {
    if (argc < 2) { 
        fprintf(stderr, "usage: %s <output.mro>\n", argv[0]); 
        return 1; 
    }
    FILE *f = fopen(argv[1], "rb"); // Apertura del file in modalità binaria
    if (!f) { 
        perror("fopen"); return 1; 
    } 

    int tlen, rlen; // Variabili per la lunghezza del token e del risultato
    while (fread(&tlen, sizeof(int), 1, f) == 1) {
        if (tlen <= 0 || tlen > 4096) { // Controllo della validità della lunghezza del token
            fprintf(stderr, "invalid token_len\n"); 
            break; 
        }
        char *token = malloc((size_t)tlen + 1);
        if (!token) break;
        if ((int)fread(token, 1, (size_t)tlen, f) != tlen) { // Lettura del token dal file
            free(token); break; 
        }
        token[tlen] = '\0';

        if (fread(&rlen, sizeof(int), 1, f) != 1) { // Lettura della lunghezza del risultato dal file
            free(token); break; 
        }
        if (rlen < 0) { // Controllo della validità della lunghezza del risultato
            free(token); 
            fprintf(stderr, "invalid result_len\n"); 
            break; 
        }

        void *result = NULL;
        if (rlen > 0) { // Se la lunghezza del risultato è maggiore di 0, allocare memoria e leggere il risultato
            result = malloc((size_t)rlen);
            if (!result) { free(token); break; }
            if ((int)fread(result, 1, (size_t)rlen, f) != rlen) { free(token); free(result); break; }
        }

        // Stampa del token e del risultato in base alla lunghezza del risultato
        printf("%s", token);
        if (rlen == (int)sizeof(long)) {
            long v; memcpy(&v, result, sizeof(long));
            printf("\t%ld", v);
        } else if (rlen == (int)sizeof(int)) {
            int v; memcpy(&v, result, sizeof(int));
            printf("\t%d", v);
        } else {
            printf("\t(bytes:%d)", rlen);
        }
        printf("\n");

        free(token);
        free(result);
    }

    fclose(f);
    return 0;
}
