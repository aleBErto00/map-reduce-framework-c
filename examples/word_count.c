#define _POSIX_C_SOURCE 200809L // Definizione per garantire la compatibilità con le funzioni POSIX
#include "../include/mr.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// Funzione di mapping che prende una riga di input, divide la riga in token e emette coppie chiave-valore
int my_mapper(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    char *buf = malloc(line->line_len + 1);
    if (!buf) return -1;
    memcpy(buf, line->line, line->line_len);
    buf[line->line_len] = '\0';

    char *saveptr;
    char *token = strtok_r(buf, " \t\r\n", &saveptr);
    int one = 1;
    while (token != NULL) {
        emit(token, &one, sizeof(int), emit_arg);
        token = strtok_r(NULL, " \t\r\n", &saveptr);
    }
    free(buf);
    return 0;
}

// Funzione di riduzione che prende un token e un array di valori associati, somma i valori e emette il risultato
int my_reducer(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg) {
    (void)user_arg;
    int sum = 0;
    for (size_t i = 0; i < values_count; i++) {
        sum += *(const int *)(values[i].data);
    }
    emit(token, &sum, sizeof(int), emit_arg);
    return 0;
}

int main() { // Funzione principale che configura e avvia il framework MapReduce
    mr_t mr; 
    mr_attr_t attr; 
    mr_attr_init(&attr); // Inizializza gli attributi del framework MapReduce con valori di default

    // Configura gli attributi del framework MapReduce a seconda delle esigenze dell'applicazione
    mr_attr_set_mapper_threads(&attr, 4);
    mr_attr_set_reducer_threads(&attr, 2);
    mr_attr_set_queue_size(&attr, 15);
    mr_attr_set_log_file(&attr, "esecuzione.log");
    mr_attr_set_stats_file(&attr, "statistiche.txt");

    if (mr_create(&mr, &attr, my_mapper, my_reducer, NULL) == 0) {
        mr_start(mr, "input_dir", "output.mro");
        mr_destroy(mr);
    }
    mr_attr_destroy(&attr);
    return 0;
}
