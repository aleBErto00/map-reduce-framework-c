#define _POSIX_C_SOURCE 200809L
#include "../include/mr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <threads.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>

// Funzioni per la lettura/scrittura sicura su pipe 
extern ssize_t readn(int fd, void *buf, size_t n);
extern ssize_t writen(int fd, const void *buf, size_t n);

typedef struct {
    double exec_time;
    unsigned long lines_read;
    unsigned long tokens_emitted;
    unsigned long couples;
    unsigned long tokens_distinct;
} mr_stats_t; // Struttura per raccogliere le statistiche di esecuzione del framework MapReduce

// Pacchetto con le statistiche calcolate dal processo
typedef struct {
    unsigned long couples;   // Inviato da Mapper
    unsigned long tokens_distinct;  // Inviato da Reducer
    unsigned long tokens_emitted;  // Inviato da Reducer
} mr_pipe_stats_t; 

struct mr { // Struttura principale che rappresenta un'istanza del framework MapReduce
    mr_attr_t attr; // Attributi di configurazione
    mr_mapper_t mapper; // Funzione Mapper fornita dall'utente
    mr_reducer_t reducer; // Funzione Reducer fornita dall'utente
    void *user_arg; // Argomento generico passato alle funzioni Mapper e Reducer
    mr_stats_t stats; // Struttura per raccogliere le statistiche di esecuzione
};

// Intestazione fissa per i pacchetti che viaggiano nelle pipe
typedef struct {
    int token_len;
    int val_size;
} pipe_packet_t;

typedef struct {
    char *file_name;
    unsigned long line_number;
    char *line;
    size_t line_len;
} queue_item_t; // Struttura per rappresentare una riga di un file inserita nella coda interna del Mapper

typedef struct {
    queue_item_t *buffer;
    size_t head;
    size_t tail;
    size_t count;
    size_t capacity;
    int stop; // Flag per indicare che non arriveranno più dati
    //Mutex e condizioni per la sincronizzazione
    mtx_t mutex;
    cnd_t not_full;
    cnd_t not_empty;
} ts_queue_t; // Struttura per una coda thread-safe usata dai Mapper

typedef struct {
    char *token;
    void **values_ptrs;
    size_t *values_sizes;
    size_t values_count;
    size_t values_capacity;
} reducer_group_t; // Struttura per rappresentare un gruppo di valori associati a una chiave, usata dal Reducer

typedef struct {
    reducer_group_t *groups;
    size_t count;
    size_t capacity;
    mtx_t mutex; // Mutex per proteggere l'accesso alla struttura durante l'accumulo dei dati
} reducer_storage_t; // Struttura per accumulare i gruppi di chiavi e valori ricevuti dal Mapper, usata dal Reducer

typedef struct {
    ts_queue_t *q;
    mr_t mr;
    int out_fd;
    mtx_t *out_mutex; // Mutex per sincronizzare l'accesso alla pipe di uscita quando più thread Mapper emettono dati
    unsigned long *couples_count; // Puntatore al contatore globale dei token emessi dai thread Mapper
} mapper_args_t; // Struttura per passare argomenti al thread del Mapper

typedef struct {
    reducer_storage_t *storage;
    mr_t mr;
    size_t thread_id;
    mtx_t *out_mutex; // Mutex per sincronizzare l'accesso alla pipe di uscita quando più thread Reducer emettono dati
    unsigned long *emitted_count; // Puntatore al contatore globale dei token emessi dai thread Reducer
} reducer_args_t; // Struttura per passare argomenti al thread del Reducer

// Struttura temporanea per raccogliere i risultati dal Reducer prima di ordinarli e scriverli su file
typedef struct {
    char *tok;
    void *res;
    int   tlen;
    int   rlen;
} out_rec_t;

static int write_stats (const char *stats_file, const mr_stats_t *stats) {
    const char *stats_filepath = stats_file;
    if (!stats_filepath) {
        stats_filepath = "mr_stats.txt"; // Nome di default ragionevole
    }

    FILE *f = fopen(stats_filepath, "w");
    if (!f) {
        perror("fopen stats file");
        return -1;
    }

    fprintf(f, "      EXECUTION STATS        \n");
    fprintf(f, "------------------------------------------\n");
    fprintf(f, "Execution time:    %.4f seconds\n", stats->exec_time);
    fprintf(f, "Lines read:        %lu\n", stats->lines_read);
    fprintf(f, "Tokens emitted:    %lu\n", stats->tokens_emitted);
    fprintf(f, "Couples processed: %lu\n", stats->couples);
    fprintf(f, "Distinct tokens:   %lu\n", stats->tokens_distinct);
    fprintf(f, "------------------------------------------\n");

    fclose(f);
    return 0;
}


// Funzione hash di default (djb2) se l'utente non ne fornisce una
static size_t default_hash(const char *token, size_t token_len, void *user_arg) {
    (void)user_arg;
    size_t hash = 5381; // Valore iniziale tipico per djb2
    for (size_t i = 0; i < token_len; i++) {
        hash = ((hash << 5) + hash) + (unsigned char)token[i]; // hash * 33 + c
    }
    return hash;
}

// Funzione di inizializzazione degli attributi con valori di default
int mr_attr_init(mr_attr_t *attr) {
    if (!attr) return -1;
    attr->mapper_threads = 1;
    attr->reducer_threads = 1;
    attr->queue_size = 10;
    attr->log_file = "mr.log";
    attr->hash = default_hash; // Imposta la funzione hash di default
    attr->hash_arg = NULL;
    return 0;
}

// Funzione di distruzione degli attributi 
int mr_attr_destroy(mr_attr_t *attr) {
    if (!attr) return -1;
    return 0;
}

// Funzioni per impostare il numero di thread mapper, con controllo di validità sui parametri
int mr_attr_set_mapper_threads(mr_attr_t *attr, size_t n) {
    if (!attr) return -1;
    if (n == 0) return -1; // Non ha senso avere 0 thread mapper
    attr->mapper_threads = n;
    return 0;
}

// Funzione per impostare il numero di thread reducer, con controllo di validità sui parametri
int mr_attr_set_reducer_threads(mr_attr_t *attr, size_t n) {
    if (!attr) return -1;
    if (n == 0) return -1; // Non ha senso avere 0 thread reducer
    attr->reducer_threads = n;
    return 0;
}
int mr_attr_set_stats_file(mr_attr_t *attr, const char *path) {
    if (!attr) return -1;
    attr->stats_file = path; // Imposta il percorso del file di statistiche
    return 0;
}

// Funzione per impostare la dimensione della coda, con controllo di validità
int mr_attr_set_queue_size(mr_attr_t *attr, size_t n) {
    if (!attr || n == 0) return -1; //Non ha senso che esista una coda di lunghezza 0
    attr->queue_size = n;
    return 0;
}

// Funzione per impostare il file di log, accettando anche un percorso NULL che resetta al valore di default
int mr_attr_set_log_file(mr_attr_t *attr, const char *path) {
    if (!attr) return -1;
    if (path) { // Se viene fornito un percorso valido, lo imposta come file di log
        attr->log_file = path;
    } else {
        attr->log_file = "mr.log"; // Reset al valore di default se viene passato NULL
    }
    return 0;
}

// Funzione per impostare la funzione hash, accettando anche un puntatore NULL che resetta alla funzione di hash di default
int mr_attr_set_hash_function(mr_attr_t *attr, mr_hash_t hash, void *hash_arg) {
    if (!attr) return -1;
    attr->hash = hash;
    if (!hash) { // Se viene passato NULL, resetta alla funzione hash di default
        attr->hash = default_hash;
    }
    attr->hash_arg = hash_arg; 
    return 0;
}

// Funzione per creare un'istanza del framework MapReduce, allocando memoria e copiando i parametri forniti
int mr_create(mr_t *mr, const mr_attr_t *attr, mr_mapper_t mapper, mr_reducer_t reducer, void *user_arg) {
    if (!mr || !attr || !mapper || !reducer) return -1;
    mr_t inst = malloc(sizeof(struct mr)); // Alloca memoria per l'istanza del framework
    if (!inst) return -1; // Controlla se l'allocazione è riuscita

    // Copia i parametri forniti nell'istanza appena allocata
    inst->attr = *attr;
    inst->mapper = mapper;
    inst->reducer = reducer;
    inst->user_arg = user_arg;
    *mr = inst; // Puntatore di output per restituire l'istanza creata
    return 0;
}

// Funzione per scrivere messaggi di log su un file specificato negli attributi.
static void write_log(const char *log_file, const char *message) { // Scrive un messaggio di log su un file specificato, con gestione della sincronizzazione tra thread
    if (!log_file || !message) return;

    static mtx_t log_mutex; // Mutex per proteggere l'accesso al file di log tra thread
    static int log_mutex_init = 0;
    if (!log_mutex_init) {
        mtx_init(&log_mutex, mtx_plain); // Inizializza il mutex solo una volta
        log_mutex_init = 1;
    }

    mtx_lock(&log_mutex); // Acquisisce il lock per scrivere sul file di log

    int fd = open(log_file, O_WRONLY | O_CREAT | O_APPEND, 0644); // Apre il file di log in modalità scrittura, creandolo se non esiste e posizionandosi alla fine per l'append
    if (fd == -1) { mtx_unlock(&log_mutex); return; }

    struct flock lock = { F_WRLCK, SEEK_SET, 0, 0, 0 }; // Imposta un lock di scrittura sul file per evitare conflitti tra processi
    fcntl(fd, F_SETLKW, &lock);
 
    time_t actual_time;
    struct tm *tm_info;
    time(&actual_time);
    tm_info = localtime(&actual_time); // Ottiene l'orario locale corrente

    char time_buffer[26];
    size_t time_len = strftime(time_buffer, sizeof(time_buffer), "%Y-%m-%d %H:%M:%S ", tm_info); // Formatta l'orario in una stringa leggibile

    writen(fd, time_buffer, time_len);
    writen(fd, message, strlen(message));

    lock.l_type = F_UNLCK;
    fcntl(fd, F_SETLK, &lock);
    close(fd);

    mtx_unlock(&log_mutex); // Rilascia il lock dopo aver scritto sul file di log
}

// Funzione per inizializzare una coda, allocando memoria e inizializzando mutex e condizioni
static int init_queue(ts_queue_t *q, size_t capacity) {
    q->buffer = malloc(capacity * sizeof(queue_item_t)); // Alloca memoria per il buffer della coda
    if (!q->buffer) { //In caso di errore nell'allocazione
        fprintf(stderr, "Allocamento di memoria per il buffer della coda non riuscito\n");
        exit(EXIT_FAILURE);
    }

    //Inizializzazione valori della coda
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    q->capacity = capacity;
    q->stop = 0;

    //Inizializzazione meccanismi di sincronizzazione della coda
    mtx_init(&q->mutex, mtx_plain);
    cnd_init(&q->not_full);
    cnd_init(&q->not_empty);

    return 0;
}

// Funzione callback usata dal Mapper per scrivere sulla pipe diretta al Reducer.
static int mapper_emit(const char *token, const void *value, size_t value_size, void *emit_arg) {
    mapper_args_t *a = (mapper_args_t *)emit_arg; // Cast dell'argomento generico alla struttura specifica per il Mapper
    
    int token_len = (int)strlen(token);
    int val_size  = (int)value_size;

    if (token_len <= 0 || token_len > 4096) return -1; //Rifiuto di dimensioni del token incompatibili
    if (val_size < 0 || val_size > (1 << 20)) return -1; //Rifiuto di dimensioni del valore del token incompatibili


    pipe_packet_t pkt = { token_len, val_size }; // Prepara il pacchetto da inviare al Reducer

    mtx_lock(a->out_mutex); //Acquisizione del lock per scrivere su file
    int ok = (writen(a->out_fd, &pkt, sizeof(pipe_packet_t)) == (ssize_t)sizeof(pipe_packet_t) &&
              writen(a->out_fd, token, (size_t)token_len) == (ssize_t)token_len &&
              writen(a->out_fd, value, value_size) == (ssize_t)value_size);
    if (ok) {
        (*(a->couples_count))++; // Aggiorna il contatore dei token emessi se la scrittura è riuscita
    }
    mtx_unlock(a->out_mutex);

    if (!ok) {
        fprintf(stderr, "Errore durante l'emissione di un token dal Mapper\n");
        return -1;
    }
    return 0;
}

// Funzione di liberazione della coda, libera la memoria allocata e distrugge mutex e condizioni
static int free_queue(ts_queue_t *q) {
    for (size_t i = 0; i < q->count; i++) { // Libera le stringhe allocate per ogni elemento nella coda
        size_t idx = (q->head + i) % q->capacity;
        free(q->buffer[idx].file_name);
        free(q->buffer[idx].line);
    }

    free(q->buffer); //Liberazione spazio dedicato al buffer
    // Distrugge il mutex e le condizioni associati alla coda
    mtx_destroy(&q->mutex); 
    cnd_destroy(&q->not_full);
    cnd_destroy(&q->not_empty);
    
    return 0;
}

// Funzione per inserire un elemento nella coda thread-safe, con gestione della sincronizzazione e del flag di stop
static void enqueue(ts_queue_t *q, queue_item_t item) {
    mtx_lock(&q->mutex);
    while (q->count == q->capacity && !q->stop) { // Attende se la coda è piena
        cnd_wait(&q->not_full, &q->mutex);
    }
    if (q->stop) { // Se è stato segnalato lo stop, non accetta più elementi e libera la memoria
        free(item.file_name);
        free(item.line);
        mtx_unlock(&q->mutex);
        return;
    }
    q->buffer[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    cnd_signal(&q->not_empty); // Segnala che la coda non è più vuota
    mtx_unlock(&q->mutex);
}

// Funzione per estrarre un elemento dalla coda thread-safe, con gestione della sincronizzazione e del flag di stop
static int dequeue(ts_queue_t *q, queue_item_t *item) {
    mtx_lock(&q->mutex);
    while (q->count == 0 && !q->stop) { // Attende se la coda è vuota
        cnd_wait(&q->not_empty, &q->mutex);
    }
    if (q->count == 0 && q->stop) { // Se è stato segnalato lo stop e la coda è vuota, termina
        mtx_unlock(&q->mutex);
        return 0;
    }
    *item = q->buffer[q->head]; // Estrae l'elemento dalla testa della coda
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    cnd_signal(&q->not_full); // Segnala che la coda non è più piena
    mtx_unlock(&q->mutex);
    return 1; // Restituisce 1 se è stato possibile estrarre un elemento, 0 se è stato segnalato lo stop
}

static int mapper_worker(void *arg) {
    mapper_args_t *m_args = (mapper_args_t *)arg; // Cast dell'argomento generico alla struttura specifica per il Mapper
    
    char tbuf[128];
    snprintf(tbuf, sizeof(tbuf), "MAPPER(tid=%lu): Thread avviato\n", (unsigned long)thrd_current());
    write_log(m_args->mr->attr.log_file, tbuf);
    
    queue_item_t item;

    while (dequeue(m_args->q, &item)) { // Estrae elementi dalla coda finché non viene segnalato lo stop
        // Prepara la struttura mr_file_line_t da passare alla funzione Mapper utente
        mr_file_line_t fl;
        // Copia i dati dell'elemento estratto dalla coda nella struttura mr_file_line_t
        fl.file_name = item.file_name; 
        fl.file_name_len = strlen(item.file_name); 
        fl.line_number = item.line_number; 
        fl.line = item.line; 
        fl.line_len = item.line_len;

        m_args->mr->mapper(&fl, mapper_emit, m_args, m_args->mr->user_arg); // Invoca la funzione Mapper fornita dall'utente

        free(item.file_name); // Libera la memoria allocata per il nome del file e la riga dopo l'elaborazione
        free(item.line);
    }

    snprintf(tbuf, sizeof(tbuf), "MAPPER(tid=%lu): Thread terminato\n", (unsigned long)thrd_current());
    write_log(m_args->mr->attr.log_file, tbuf);

    return 0; // Termina il thread del Mapper quando viene segnalato lo stop
}

// Ciclo del processo Mapper
static void run_mapper_process(mr_t mr) {
    static unsigned long mapper_couples = 0; // Contatore globale per il numero di coppie emesse dai thread Mapper

    write_log(mr->attr.log_file, "MAPPER: Processo avviato\n"); // Scrive un messaggio di log per indicare l'avvio del processo Mapper
    ts_queue_t q; 
    init_queue(&q, mr->attr.queue_size); 

    mtx_t out_mutex; // Mutex per sincronizzare l'accesso alla pipe di uscita quando più thread Mapper emettono dati
    mtx_init(&out_mutex, mtx_plain); 
    thrd_t *threads = malloc(mr->attr.mapper_threads * sizeof(thrd_t)); 
    mapper_args_t m_args = { &q, mr, STDOUT_FILENO, &out_mutex, &mapper_couples}; 

    for (size_t i = 0; i < mr->attr.mapper_threads; i++) { // Crea i thread del Mapper
        thrd_create(&threads[i], mapper_worker, &m_args);
    }

    // Estrazione dei dati del file di input inviati dal processo Main tramite la pipe e inserimento nella coda thread-safe
    char *f_name = NULL;
    size_t f_name_len = 0; 
    unsigned long l_num = 0; 
    size_t l_len = 0; 
    unsigned long lines_read = 0;

    // Ciclo principale del processo Mapper: legge i dati serializzati inviati dal processo Main e li inserisce nella coda thread-safe
    while (readn(STDIN_FILENO, &f_name_len, sizeof(size_t)) == sizeof(size_t)) { // Legge i dati serializzati inviati dal processo main
        f_name = malloc(f_name_len + 1);
        readn(STDIN_FILENO, f_name, f_name_len); // Legge il nome del file dalla pipe e lo memorizza in f_name
        f_name[f_name_len] = '\0'; 

        // Legge il numero della riga e la lunghezza della riga dalla pipe
        readn(STDIN_FILENO, &l_num, sizeof(unsigned long)); // Legge il numero della riga
        readn(STDIN_FILENO, &l_len, sizeof(size_t)); // Legge la lunghezza della riga

        // Alloca memoria per la riga e legge la riga vera e propria dalla pipe
        char *line = malloc(l_len + 1);
        readn(STDIN_FILENO, line, l_len); // Legge la riga vera e propria dalla pipe
        line[l_len] = '\0'; // Legge la riga vera e propria fino alla lunghezza specificata

        queue_item_t item = { f_name, l_num, line, l_len }; // Crea un elemento della coda con i dati letti 
        enqueue(&q, item); // Inserisce l'elemento nella coda thread-safe per essere elaborato dai thread Mapper
        lines_read++;
    }

    mtx_lock(&q.mutex);
    q.stop = 1; // Segnala ai thread Mapper che non arriveranno più dati
    cnd_broadcast(&q.not_empty);
    mtx_unlock(&q.mutex);

    for (size_t i = 0; i < mr->attr.mapper_threads; i++) {
        thrd_join(threads[i], NULL); // Attende che tutti i thread Mapper abbiano terminato
    }

    free(threads);
    mtx_destroy(&out_mutex);
    free_queue(&q);

    char lbuf[128];
    snprintf(lbuf, sizeof(lbuf), "MAPPER: Righe elaborate: %lu\n", lines_read);
    write_log(mr->attr.log_file, lbuf);
    write_log(mr->attr.log_file, "MAPPER: Processo terminato\n");


    pipe_packet_t stats_pkt = { -1, (int)mapper_couples }; // Pacchetto speciale per inviare le statistiche al processo Main
    writen(STDOUT_FILENO, &stats_pkt, sizeof(pipe_packet_t)); // Prepara un pacchetto di statistiche da inviare al processo Main
    _exit(0); // Termina il processo Mapper; la chiusura di STDOUT segnala EOF al Reducer
}


// Funzione di confronto per qsort usata per ordinare i gruppi di chiavi del Reducer in ordine lessicografico
static int compare_groups(const void *a, const void *b) {
    return strcmp(((reducer_group_t *)a)->token, ((reducer_group_t *)b)->token);
}

// Funzione per aggiungere un token e il suo valore alla struttura di accumulo del Reducer.
static void add_storage(reducer_storage_t *st, const char *token, void *value, size_t value_size) {
    mtx_lock(&st->mutex);
    reducer_group_t *g = NULL; // Puntatore al gruppo corrispondente al token, se esiste

    // Cerca se esiste già un gruppo per questo token
    for (size_t i = 0; i < st->count; i++) {
        if (strcmp(st->groups[i].token, token) == 0) {
            g = &st->groups[i];
            break;
        }
    }

    if (!g) { // Se non esiste ancora un gruppo per questo token, ne crea uno nuovo
        if (st->count == st->capacity) {
            st->capacity *= 2; 
            st->groups = realloc(st->groups, st->capacity * sizeof(reducer_group_t)); // Raddoppia la capacità se necessario
        }
        g = &st->groups[st->count++]; // Ottiene un puntatore al nuovo gruppo e lo inizializza
        g->token = strdup(token); 
        g->values_capacity = 4;
        g->values_count = 0;
        g->values_ptrs  = malloc(4 * sizeof(void *)); // Alloca memoria per i puntatori ai valori
        g->values_sizes = malloc(4 * sizeof(size_t)); // Alloca memoria per le dimensioni dei valori
    }
    if (g->values_count == g->values_capacity) { // Se il gruppo ha raggiunto la capacità massima, raddoppia
        g->values_capacity *= 2;
        g->values_ptrs  = realloc(g->values_ptrs,  g->values_capacity * sizeof(void *));
        g->values_sizes = realloc(g->values_sizes, g->values_capacity * sizeof(size_t));
    }
    // Copia del valore opaco: gestisce val_size==0 senza chiamare malloc(0) o memcpy con NULL
    void *copy = NULL;
    if (value_size > 0) {
        copy = malloc(value_size);
        memcpy(copy, value, value_size);
    }

    // Aggiunge il puntatore al valore copiato e la sua dimensione al gruppo
    g->values_ptrs[g->values_count]  = copy;
    g->values_sizes[g->values_count] = value_size;
    g->values_count++;
    mtx_unlock(&st->mutex); // Rilascia il mutex dopo aver modificato la struttura di accumulo del Reducer
}

// Funzione callback usata dal Reducer per scrivere i risultati sulla pipe diretta al processo Main.
static int reducer_emit(const char *token, const void *result, size_t result_size, void *emit_arg) {
    
    // Inizializzazione variabili locali e preparazione del pacchetto da inviare al processo Main
    reducer_args_t *a = (reducer_args_t *)emit_arg;
    int token_len  = (int)strlen(token);
    int result_len = (int)result_size;
    pipe_packet_t pkt = { token_len, result_len }; 

    mtx_lock(a->out_mutex);
    int ok = (writen(STDOUT_FILENO, &pkt, sizeof(pipe_packet_t)) == (ssize_t)sizeof(pipe_packet_t) &&
              writen(STDOUT_FILENO, token, (size_t)token_len) == (ssize_t)token_len &&
              writen(STDOUT_FILENO, result, result_size) == (ssize_t)result_size);
    if (ok) {
        (*(a->emitted_count))++; // Aggiorna il contatore dei token emessi se la scrittura è riuscita
    }
    mtx_unlock(a->out_mutex);
    if (!ok) {
        fprintf(stderr, "Errore durante l'emissione di un token dal Reducer\n");
        return -1;
    }
    return 0;
}

// Funzione worker per i thread del Reducer: processa i gruppi assegnati tramite hash e invoca il Reducer utente
static int reducer_worker(void *arg) {
    reducer_args_t *r_args = (reducer_args_t *)arg;

    char tbuf[128];
    snprintf(tbuf, sizeof(tbuf), "REDUCER(tid=%lu): Thread avviato\n", (unsigned long)thrd_current());
    write_log(r_args->mr->attr.log_file, tbuf);

    reducer_storage_t *st = r_args->storage; 
    mr_t mr = r_args->mr;

    for (size_t i = 0; i < st->count; i++) { // Per ogni gruppo, decide se questo thread è responsabile tramite hash
        size_t h = mr->attr.hash(st->groups[i].token, strlen(st->groups[i].token), mr->attr.hash_arg); 

        if ((h % mr->attr.reducer_threads) == r_args->thread_id) { // Se il thread corrente è responsabile di questo gruppo, chiama il Reducer utente
            size_t v_count = st->groups[i].values_count;
            mr_value_t *m_vals = malloc(v_count * sizeof(mr_value_t));
            for (size_t j = 0; j < v_count; j++) { // Prepara l'array di mr_value_t da passare alla funzione Reducer utente
                m_vals[j].data = st->groups[i].values_ptrs[j];
                m_vals[j].size = st->groups[i].values_sizes[j];
            }
            mr->reducer(st->groups[i].token, m_vals, v_count, reducer_emit, r_args, mr->user_arg); 
            free(m_vals); // Libera la memoria allocata per l'array di mr_value_t dopo aver chiamato il Reducer
        }
    }

    snprintf(tbuf, sizeof(tbuf), "REDUCER(tid=%lu): Thread terminato\n", (unsigned long)thrd_current());
    write_log(r_args->mr->attr.log_file, tbuf);

    return 0;
}

// Ciclo del processo Reducer: accumula le coppie ricevute dal Mapper, raggruppa, ordina, riduce
static void run_reducer_process(mr_t mr) {
    write_log(mr->attr.log_file, "REDUCER: Processo avviato\n");
    reducer_storage_t st; // Struttura per accumulare i gruppi di chiavi e valori ricevuti dal Mapper
    st.groups = malloc(16 * sizeof(reducer_group_t)); // Alloca memoria per i gruppi di chiavi e valori
    st.count = 0;
    st.capacity = 16; 

    unsigned long reducer_emitted = 0; // Contatore globale per il numero di token emessi dai thread Reducer
    unsigned long mapper_couples = 0; // Variabile locale per il numero di coppie ricevute dal Mapper

    pipe_packet_t pkt; // Struttura temporanea per leggere i pacchetti inviati dal Mapper
    mtx_init(&st.mutex, mtx_plain); // Inizializza il mutex per proteggere la struttura di accumulo

    // Legge le coppie (chiave, valore) inviate dal Mapper finché non riceve EOF
    while (readn(STDIN_FILENO, &pkt, sizeof(pipe_packet_t)) == sizeof(pipe_packet_t)) {
        if (pkt.token_len == -1) { // Pacchetto speciale per le statistiche del Mapper
            mapper_couples = (unsigned long)pkt.val_size; // Aggiorna il numero di coppie ricevute dal Mapper
            continue;
        }

        if (pkt.token_len <= 0 || pkt.token_len > 4096) {
            write_log(mr->attr.log_file, "REDUCER: Token di lunghezza non valida ricevuto, ignorato\n");
            break;
        }
        if (pkt.val_size < 0 || pkt.val_size > (1 << 20)) {
            write_log(mr->attr.log_file, "REDUCER: Valore di dimensione non valida ricevuto, ignorato\n");
            break;
        }

        char *token = malloc((size_t)pkt.token_len + 1); // Alloca memoria per il token e legge il token dalla pipe
        readn(STDIN_FILENO, token, (size_t)pkt.token_len);
        token[pkt.token_len] = '\0';

        // Alloca memoria per il valore e legge il valore dalla pipe, gestendo correttamente val_size==0
        void *val_data = NULL;
        if (pkt.val_size > 0) {
            val_data = malloc((size_t)pkt.val_size); 
            readn(STDIN_FILENO, val_data, (size_t)pkt.val_size);
        }

        // Aggiunge il token e il valore alla struttura di accumulo del Reducer
        add_storage(&st, token, val_data, (size_t)pkt.val_size);
        free(token);
        free(val_data);
    }


    qsort(st.groups, st.count, sizeof(reducer_group_t), compare_groups); // Ordina i gruppi lessicograficamente

    // Log del numero di token distinti accumulati dal Reducer
    char rbuf[128];
    snprintf(rbuf, sizeof(rbuf), "REDUCER: Gruppi distinti: %zu\n", st.count);
    write_log(mr->attr.log_file, rbuf);

    mtx_t out_mutex; // Mutex per sincronizzare l'accesso alla pipe di uscita tra più thread Reducer
    mtx_init(&out_mutex, mtx_plain);
    thrd_t *threads = malloc(mr->attr.reducer_threads * sizeof(thrd_t)); 
    reducer_args_t *args_array = malloc(mr->attr.reducer_threads * sizeof(reducer_args_t));

    for (size_t i = 0; i < mr->attr.reducer_threads; i++) { // Crea i thread del Reducer
        args_array[i].storage = &st;
        args_array[i].mr = mr;
        args_array[i].thread_id = i;
        args_array[i].out_mutex = &out_mutex;
        args_array[i].emitted_count = &reducer_emitted;
        thrd_create(&threads[i], reducer_worker, &args_array[i]);
    }

    for (size_t i = 0; i < mr->attr.reducer_threads; i++) { // Attende che tutti i thread del Reducer abbiano terminato
        thrd_join(threads[i], NULL);
    }

    for (size_t i = 0; i < st.count; i++) { // Libera la memoria allocata per i gruppi
        free(st.groups[i].token);
        for (size_t j = 0; j < st.groups[i].values_count; j++) { 
            free(st.groups[i].values_ptrs[j]);
        }
        free(st.groups[i].values_ptrs);
        free(st.groups[i].values_sizes);
    }
    free(st.groups);
    mtx_destroy(&out_mutex);
    free(threads);
    free(args_array);
    mtx_destroy(&st.mutex);

    mr_pipe_stats_t final_stats;
    final_stats.couples = mapper_couples;
    final_stats.tokens_distinct = st.count;
    final_stats.tokens_emitted = reducer_emitted;

    pipe_packet_t stats_pkt = { -1, sizeof(mr_pipe_stats_t) };
    writen(STDOUT_FILENO, &stats_pkt, sizeof(pipe_packet_t)); // Prepara un pacchetto di statistiche da inviare al processo Main
    writen(STDOUT_FILENO, &final_stats, sizeof(mr_pipe_stats_t)); // Invia le statistiche finali al processo Main
    write_log(mr->attr.log_file, "REDUCER: Processo terminato\n");
    _exit(0);
}

// Funzione per scansionare un file e inviare le righe al Mapper tramite la pipe, leggendo il file riga per riga
static int scan_and_send_file(mr_t mr, const char *path, int out_fd) {
    char lbuf[256];
    snprintf(lbuf, sizeof(lbuf), "MAIN: Scansione file: %s\n", path);
    write_log(mr->attr.log_file, lbuf);
    
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char *line = NULL;
    size_t len = 0;
    ssize_t nread;
    unsigned long line_num = 0;
    size_t path_len = strlen(path);
    while ((nread = getline(&line, &len, f)) != -1) {
        line_num++;
        if (nread > 0 && line[nread - 1] == '\n') { 
            line[nread - 1] = '\0'; 
            nread--; 
        } 
        size_t l_len = (size_t)nread;
        writen(out_fd, &path_len, sizeof(size_t));
        writen(out_fd, path, path_len);
        writen(out_fd, &line_num, sizeof(unsigned long));
        writen(out_fd, &l_len, sizeof(size_t));
        writen(out_fd, line, l_len);
        mr->stats.lines_read++; // Aggiorna il contatore globale delle righe lette
    }

    free(line);

    snprintf(lbuf, sizeof(lbuf), "MAIN: Chiusura file: %s, righe lette: %lu\n", path, line_num);
    write_log(mr->attr.log_file, lbuf);
    fclose(f);
    return 0;
}

static int compare_file_paths(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static void collect_files(const char *dir_path, char ***file_list, size_t *file_count, size_t *file_capacity) {
    struct stat st;
    if (stat(dir_path, &st) == -1) return;

    // se è un File regolare
    if (S_ISREG(st.st_mode)) {
        if (*file_count == *file_capacity) { // Se l'array è pieno, raddoppia la capacità
            size_t new_cap = *file_capacity * 2;
            char **tmp = realloc(*file_list, new_cap * sizeof(char *));
            if (!tmp) return; // Se realloc fallisce, mantiene l'array precedente senza crashare
            *file_list = tmp;
            *file_capacity = new_cap;
        }

        char *path_dup = strdup(dir_path); // Duplica il percorso del file per memorizzarlo nell'array
        if (path_dup) {
            (*file_list)[(*file_count)++] = path_dup;
        }
        return;
    }

    // se è una Directory 
    if (S_ISDIR(st.st_mode)) {
        DIR *d = opendir(dir_path);
        if (!d) return;

        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            // Ignora "." e ".." per evitare cicli infiniti
            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

            char full_path[4096]; // Buffer per costruire il percorso completo del file o della directory
            size_t len = strlen(dir_path);
            if (len > 0 && dir_path[len - 1] == '/') {
                snprintf(full_path, sizeof(full_path), "%s%s", dir_path, de->d_name);
            } else {
                snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, de->d_name);
            }

            collect_files(full_path, file_list, file_count, file_capacity);
        }
        closedir(d);
    }
}

static void process_input_path(mr_t mr, const char *input_path, int out_fd) {
    size_t count = 0;
    size_t capacity = 16;
    char **entries = malloc(capacity * sizeof(char *));

    if (!entries) return;

    // Raccoglie tutti i file regolari ricorsivamente
    collect_files(input_path, &entries, &count, &capacity);

    if (count > 0) {
        // Ordina tutti i percorsi trovati in ordine lessicografico deterministico
        qsort(entries, count, sizeof(char *), compare_file_paths);

        //  Elabora i file ordinati e libera la memoria
        for (size_t i = 0; i < count; i++) {
            scan_and_send_file(mr, entries[i], out_fd);
            free(entries[i]);
        }
    }

    free(entries);
}


// Funzione di confronto per ordinare i record di output lessicograficamente per token.
// Usata da qsort prima della scrittura sul file di output, garantendo determinismo.
static int compare_out_records(const void *a, const void *b) {
    return strcmp(((const out_rec_t *)a)->tok, ((const out_rec_t *)b)->tok);
}

int mr_start(mr_t mr, const char *input_path, const char *output_path) { // Funzione principale per avviare il framework MapReduce
    if (!mr || !input_path || !output_path) return -1;
    write_log(mr->attr.log_file, "MAIN: Avvio framework\n");

    struct timespec start_time, end_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time); // Inizio del timer per misurare il tempo di esecuzione del framework

    // Creazione delle pipe per la comunicazione tra i processi Main, Mapper e Reducer
    int main_to_mapper[2]; 
    int mapper_to_reducer[2];
    int reducer_to_main[2];

    // Creazione delle 3 pipe richieste prima di qualsiasi fork
    if (pipe(main_to_mapper) == -1 || pipe(mapper_to_reducer) == -1 || pipe(reducer_to_main) == -1) {
        return -1;
    }
    write_log(mr->attr.log_file, "MAIN: Pipe create\n");

    // Fork del processo Mapper (avviene prima di qualsiasi thrd_create)
    pid_t m_pid = fork();
    if (m_pid == 0) {
        dup2(main_to_mapper[0], STDIN_FILENO);
        dup2(mapper_to_reducer[1], STDOUT_FILENO);
        close(main_to_mapper[0]); 
        close(main_to_mapper[1]);
        close(mapper_to_reducer[0]);
        close(reducer_to_main[0]);
        close(mapper_to_reducer[1]); 
        close(reducer_to_main[1]);
        run_mapper_process(mr); 
    } else if (m_pid < 0) {
        return -1; // Errore nel fork del processo Mapper
    }
    write_log(mr->attr.log_file, "MAIN: Mapper avviato\n");

    // Fork del processo Reducer (avviene prima di qualsiasi thrd_create)
    pid_t r_pid = fork();
    if (r_pid == 0) {
        dup2(mapper_to_reducer[0], STDIN_FILENO);
        dup2(reducer_to_main[1], STDOUT_FILENO);
        close(main_to_mapper[0]); 
        close(main_to_mapper[1]);
        close(mapper_to_reducer[0]); 
        close(mapper_to_reducer[1]); 
        close(reducer_to_main[0]);
        close(reducer_to_main[1]);
        run_reducer_process(mr);
    } else if (r_pid < 0) {
        return -1; // Errore nel fork del processo Reducer
    }
    write_log(mr->attr.log_file, "MAIN: Reducer avviato\n");

    // Processo principale: chiude i descrittori non necessari
    close(main_to_mapper[0]);
    close(mapper_to_reducer[0]); 
    close(mapper_to_reducer[1]);
    close(reducer_to_main[1]);

    // Invia i dati al mapper; la chiusura di main_to_mapper[1] segnala EOF al Mapper
    process_input_path(mr, input_path, main_to_mapper[1]);
    close(main_to_mapper[1]); // Segnala EOF al mapper chiudendo la pipe

    // Raccoglie i risultati dal Reducer in memoria, li ordina lessicograficamente, poi scrive su file
    out_rec_t *recs = NULL;
    size_t rec_count = 0, rec_cap = 0;
    pipe_packet_t pkt;

    while (readn(reducer_to_main[0], &pkt, sizeof(pipe_packet_t)) == (ssize_t)sizeof(pipe_packet_t)) {
        
        if (pkt.token_len == -1) { // Pacchetto speciale per le statistiche del Reducer
            mr_pipe_stats_t stats;
            if (readn(reducer_to_main[0], &stats, sizeof(mr_pipe_stats_t))) {
                mr->stats.couples = stats.couples;
                mr->stats.tokens_distinct = stats.tokens_distinct;
                mr->stats.tokens_emitted = stats.tokens_emitted;
            }
                continue;
        }

        if (pkt.token_len <= 0 || pkt.token_len > 4096) break;
        if (pkt.val_size < 0 || pkt.val_size > (1 << 20)) break;

        char *tok = malloc((size_t)pkt.token_len + 1);
        readn(reducer_to_main[0], tok, (size_t)pkt.token_len);
        tok[pkt.token_len] = '\0';
        void *res = NULL;

        if (pkt.val_size > 0) { // Alloca memoria per il risultato e legge il risultato dalla pipe, gestendo correttamente val_size==0
            res = malloc((size_t)pkt.val_size);
            readn(reducer_to_main[0], res, (size_t)pkt.val_size);
        }

        if (rec_count == rec_cap) { // Raddoppia la capacità dell'array di record se necessario
            if (rec_cap == 0) {
                rec_cap = 16; 
            } else {
                rec_cap *= 2;
            }
            recs = realloc(recs, rec_cap * sizeof(out_rec_t));
        }

        // Copia i dati nel record di output
        recs[rec_count].tok  = tok;
        recs[rec_count].res  = res;
        recs[rec_count].tlen = pkt.token_len;
        recs[rec_count].rlen = pkt.val_size;
        rec_count++;
    }
    
    close(reducer_to_main[0]);

    // Ordinamento lessicografico dei risultati per garantire output deterministico
    qsort(recs, rec_count, sizeof(out_rec_t), compare_out_records);

    // Scrittura del file di output in formato binario: [tlen:int][token][rlen:int][result]
    int out_file = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_file != -1) {
        for (size_t i = 0; i < rec_count; i++) {
            writen(out_file, &recs[i].tlen, sizeof(int));
            writen(out_file, recs[i].tok,  (size_t)recs[i].tlen);
            writen(out_file, &recs[i].rlen, sizeof(int));
            if (recs[i].rlen > 0) writen(out_file, recs[i].res, (size_t)recs[i].rlen);
        }
        close(out_file);
    }

    char obuf[128];
    snprintf(obuf, sizeof(obuf), "MAIN: Risultati scritti: %zu\n", rec_count);
    write_log(mr->attr.log_file, obuf);

    // Libera la memoria dei record di output
    for (size_t i = 0; i < rec_count; i++) { free(recs[i].tok); free(recs[i].res); }
    free(recs);

    // Attende che i processi figli abbiano finito per ripulire il sistema ed evitare processi zombie
    waitpid(m_pid, NULL, 0);
    waitpid(r_pid, NULL, 0);




    clock_gettime(CLOCK_MONOTONIC, &end_time);
    mr->stats.exec_time = (end_time.tv_sec - start_time.tv_sec) + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;

    write_stats("mr_stats.txt", &mr->stats); // Scrive le statistiche finali sul file di log
    
    write_log(mr->attr.log_file, "MAIN: Framework terminato\n");
    return 0;
}

int mr_destroy(mr_t mr) { // Funzione per distruggere l'istanza del framework MapReduce, liberando la memoria allocata
    if (!mr) return -1;
    free(mr);
    return 0;
}
