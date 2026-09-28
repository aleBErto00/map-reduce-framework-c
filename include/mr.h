#ifndef MR_H
#define MR_H

#include <stddef.h>

typedef struct mr *mr_t; 

typedef size_t (*mr_hash_t)(const char *token, size_t token_len, void *user_arg); //Tipo della funzione hash che prende un token e restituisce un valore hash

typedef struct {
    size_t mapper_threads;
    size_t reducer_threads;
    size_t queue_size;
    const char *log_file;
    const char *stats_file;
    mr_hash_t hash;
    void *hash_arg;
} mr_attr_t; // Struttura per gli attributi del framework MapReduce

typedef struct {
    const char *file_name;
    size_t file_name_len;
    unsigned long line_number;
    const char *line;
    size_t line_len;
} mr_file_line_t; // Struttura per rappresentare una riga di un file di input

typedef struct {
    const void *data;
    size_t size;
} mr_value_t; // Struttura per rappresentare un valore nel framework MapReduce

typedef int (*mr_emit_pair_t)(const char *token, const void *value, size_t value_size, void *emit_arg);
typedef int (*mr_emit_result_t)(const char *token, const void *result, size_t result_size, void *emit_arg);

typedef int (*mr_mapper_t)(const mr_file_line_t *line, mr_emit_pair_t emit, void *emit_arg, void *user_arg);
typedef int (*mr_reducer_t)(const char *token, const mr_value_t *values, size_t values_count, mr_emit_result_t emit, void *emit_arg, void *user_arg);

// Funzioni per la gestione degli attributi del framework MapReduce
int mr_attr_init(mr_attr_t *attr);
int mr_attr_destroy(mr_attr_t *attr);
int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n);
int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n);
int mr_attr_set_queue_size(mr_attr_t *attr, size_t n);
int mr_attr_set_log_file(mr_attr_t *attr, const char *path);
int mr_attr_set_hash_function(mr_attr_t *attr, mr_hash_t hash, void *hash_arg);
int mr_attr_set_stats_file(mr_attr_t *attr, const char *path);

// Funzioni per la gestione del framework MapReduce
int mr_create(mr_t *mr, const mr_attr_t *attr, mr_mapper_t mapper, mr_reducer_t reducer, void *user_arg);
int mr_start(mr_t mr, const char *input_path, const char *output_path);
int mr_destroy(mr_t mr);

#endif
