#define _POSIX_C_SOURCE 200809L // Definizione per abilitare le funzionalità POSIX
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include "../include/mr.h"

#define TOKEN_MAX 4096 // Lunghezza massima di un token
#define PASS(n) fprintf(stdout, "[PASS] %s\n", n) // Macro per stampare un messaggio di test superato
#define FAIL(n) fprintf(stdout, "[FAIL] %s\n", n) // Macro per stampare un messaggio di test fallito

// Funzione Mapper di base che legge una riga di input, estrae i token alfanumerici e li emette con un valore intero 1
static int basic_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    const char *p = line->line;
    size_t rem = line->line_len; // Lunghezza rimanente della riga da processare
    char token[TOKEN_MAX + 1];
    while (rem > 0) { // Scansiona la riga per trovare token alfanumerici
        while (rem > 0 && !isalnum((unsigned char)*p)) { p++; rem--; }
        if (rem == 0) break;
        size_t tlen = 0;
        // Copia i caratteri alfanumerici nel buffer del token, convertendoli in minuscolo e limitando la lunghezza a TOKEN_MAX
        while (rem > 0 && isalnum((unsigned char)*p) && tlen < TOKEN_MAX) { 
            token[tlen++] = (char)tolower((unsigned char)*p);
            p++; rem--;
        }
        token[tlen] = '\0'; // Termina la stringa del token
        if (tlen == 0) continue;
        int one = 1;
        emit(token, &one, sizeof(int), emit_arg); // Emette il token con il valore 1
    }
    return 0;
}

// Funzione Reducer di base che somma i valori associati a un token e li emette come long
static int basic_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    long total = 0;
    for (size_t i = 0; i < values_count; i++) {
        // Verifica se il valore è un intero e lo somma al totale
        if (values[i].size == sizeof(int)) {
            int v; memcpy(&v, values[i].data, sizeof(int)); total += v; // Somma il valore al totale
        }
    }
    emit(token, &total, sizeof(long), emit_arg);
    return 0;
}

// Funzione di test che esegue il framework MapReduce con input specificato e verifica se il token atteso ha il conteggio corretto
static void test(const char *name, const char *input, const char *expected_token, long expected_count) {
    char in_path[]  = "/tmp/mr_in_XXXXXX";
    char out_path[] = "/tmp/mr_out_XXXXXX";
    int fd = mkstemp(in_path);
    ssize_t nw = write(fd, input, strlen(input)); (void)nw;
    close(fd);
    fd = mkstemp(out_path); close(fd);


    // Configura e avvia il framework MapReduce con i mapper e reducer di base
    mr_t mr; mr_attr_t attr;
    mr_attr_init(&attr);
    mr_attr_set_mapper_threads(&attr, 2);
    mr_attr_set_reducer_threads(&attr, 2);
    mr_attr_set_queue_size(&attr, 16);
    mr_attr_set_log_file(&attr, "/dev/null");
    mr_create(&mr, &attr, basic_mapper, basic_reducer, NULL);
    int r = mr_start(mr, in_path, out_path);
    mr_destroy(mr); mr_attr_destroy(&attr);

    if (r != 0) { // Se il framework non è riuscito a completare correttamente, segnala il test come fallito
        FAIL(name); 
        unlink(in_path);
        unlink(out_path); 
        return; 
    }

    // Legge il file di output e verifica se il token atteso ha il conteggio corretto
    FILE *f = fopen(out_path, "rb");
    int found = 0;
    if (f) { // Se il file di output è stato aperto correttamente, cerca il token atteso
        int tlen, rlen;
        while (fread(&tlen, sizeof(int), 1, f) == 1) {
            if (tlen <= 0 || tlen > TOKEN_MAX) break;
            char *tok = malloc((size_t)tlen + 1);
            size_t nr1 = fread(tok, 1, (size_t)tlen, f); // Legge il token dal file di output
            tok[tlen] = '\0'; 
            (void)nr1;
            size_t nr2 = fread(&rlen, sizeof(int), 1, f); // Legge la lunghezza del valore associato al token
            (void)nr2;
            void *res = NULL;
            if (rlen > 0) {
                res = malloc((size_t)rlen);
                size_t nr3 = fread(res, 1, (size_t)rlen, f); (void)nr3;
            }
            if (strcmp(tok, expected_token) == 0 && rlen == (int)sizeof(long)) { // Se il token corrisponde a quello atteso e la lunghezza del valore è corretta, verifica il conteggio
                long v; memcpy(&v, res, sizeof(long));
                if (v == expected_count) found = 1;
            }
            free(tok); free(res);
        }
        fclose(f);
    }

    // Segnala il risultato del test in base al fatto se il token atteso è stato trovato con il conteggio corretto
    if (found)  {
        PASS(name); 
    }
    else {
        FAIL(name);
    }

    // Pulisce i file temporanei creati per il test
    unlink(in_path); 
    unlink(out_path);
}

// Funzione di test per verificare il comportamento del framework con un file di input vuoto
static void test_empty_file(void) {
    char in_path[] = "/tmp/mr_in_AAAAAA"; // Crea un file temporaneo per l'input
    char out_path[] = "/tmp/mr_out_AAAAAA"; // Crea un file temporaneo per l'output
    int fd = mkstemp(in_path); close(fd);

    fd = mkstemp(out_path); close(fd);

    mr_t mr; mr_attr_t attr;

    mr_attr_init(&attr); mr_attr_set_log_file(&attr, "/dev/null"); 
    mr_create(&mr, &attr, basic_mapper, basic_reducer, NULL);
    int r = mr_start(mr, in_path, out_path);
    mr_destroy(mr); mr_attr_destroy(&attr);

    if (r == 0) {
        PASS("empty_file"); 
    } else {
        FAIL("empty_file"); 
    }

    unlink(in_path); 
    unlink(out_path);
}

// Funzione di test per verificare il comportamento del framework con valori di attributi non validi
static void test_attr_invalid(void) {
    mr_attr_t attr; mr_attr_init(&attr);
    int r1 = mr_attr_set_mapper_threads(&attr, 0); // Non ha senso avere 0 thread mapper
    int r2 = mr_attr_set_queue_size(&attr, 0); // Non ha senso avere una coda di dimensione 0
    if (r1 == -1 && r2 == -1) {
        PASS("attr_invalid_values");
    } else {
        FAIL("attr_invalid_values");
    }
}

// Funzione di test per verificare che l'output del framework sia ordinato lessicograficamente
static void test_output_sorted(void) {
    char in_path[]  = "/tmp/mr_in_AAAAAA"; // Crea un file temporaneo per l'input
    char out_path[] = "/tmp/mr_out_AAAAAA"; // Crea un file temporaneo per l'output
    int fd = mkstemp(in_path);
    const char *data = "casa albero casa mela mela albero casa \n"; // Dati di input per il test
    ssize_t nw = write(fd, data, strlen(data)); (void)nw; // Scrive i dati di input nel file temporaneo
    close(fd);
    fd = mkstemp(out_path); close(fd);

    mr_t mr; mr_attr_t attr;
    mr_attr_init(&attr);
    mr_attr_set_reducer_threads(&attr, 3);
    mr_attr_set_log_file(&attr, "/dev/null");
    mr_create(&mr, &attr, basic_mapper, basic_reducer, NULL);
    mr_start(mr, in_path, out_path);
    mr_destroy(mr); mr_attr_destroy(&attr);

    // Verifica se l'output è ordinato lessicograficamente confrontando i token in sequenza
    FILE *f = fopen(out_path, "rb");
    char prev[TOKEN_MAX + 1]; prev[0] = '\0';
    int sorted = 1, tlen, rlen; // Flag per indicare se l'output è ordinato

    while (fread(&tlen, sizeof(int), 1, f) == 1) {
        if (tlen <= 0 || tlen > TOKEN_MAX) break;
        char tok[TOKEN_MAX + 1];
        size_t nr1 = fread(tok, 1, (size_t)tlen, f); tok[tlen] = '\0'; (void)nr1;
        size_t nr2 = fread(&rlen, sizeof(int), 1, f); (void)nr2;
        if (rlen > 0) { void *r2 = malloc((size_t)rlen); size_t nr3 = fread(r2, 1, (size_t)rlen, f); (void)nr3; free(r2); }
        if (strcmp(tok, prev) < 0) { sorted = 0; break; }
        memcpy(prev, tok, (size_t)tlen + 1); // Aggiorna il token precedente per il confronto successivo
    }

    fclose(f);
    if (sorted) {
        PASS("output_sorted");
    } else {
        FAIL("output_sorted");
    }
    unlink(in_path); 
    unlink(out_path);
}

// Funzione principale che esegue tutti i test definiti sopra
int main(void) { 
    test("single_word",   "hello\n",            "hello", 1); // Test con una singola parola
    test("repeated_word", "foo foo foo\n",      "foo",   3); // Test con una parola ripetuta
    test("multiline",     "cat\ndog\ncat\n",    "cat",   2); // Test con più righe
    test("no_newline_end","word",               "word",  1); // Test senza newline alla fine
    test("mixed_case",    "Hello hello HELLO\n","hello", 3); // Test con lettere maiuscole e minuscole
    test("empty_line",    "\nhello\n\nhello\n", "hello", 2); // Test con righe vuote
    test_empty_file();
    test_attr_invalid();
    test_output_sorted();
    return 0;
}
