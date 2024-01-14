/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define BUFFER_SIZE 512

// TODO: Recognize TAGs related to
// barriers, broadcasts, reductions
#define BARRIER_TAG -2
#define BROADCAST_TAG -3
#define REDUCTION_TAG -4

// buffor -> list of messages
typedef struct message_buf {
    int process_id;
    int tag;
    int count;
    void *data;
    struct message_buf *next;
    struct message_buf *prev;
} message_buf;

static int rank;
static int size;

static pthread_mutex_t mutex;
static pthread_cond_t cond;
static pthread_t *read_threads;
static message_buf *message_buffer;
static bool *finished_processes;
static int *thread_args;

void init(message_buf *buf) {
    buf->process_id = -1;
    buf->tag = -1;
    buf->count = -1;
    buf->data = NULL;
    buf->next = NULL;
    buf->prev = NULL;
}

message_buf *create_message_buf(int process_id, int tag, int count, void *data) {
    message_buf *buf = malloc(sizeof(message_buf));
    buf->process_id = process_id;
    buf->tag = tag;
    buf->count = count;
    buf->data = data;
    buf->next = NULL;
    buf->prev = NULL;
    return buf;
}

void add_message(message_buf *buf, message_buf *new_message) {
    if (buf->next == NULL) {
        buf->next = new_message;
        new_message->prev = buf;
    } else {
        add_message(buf->next, new_message);
    }
}

//TODO: free buf->data
void remove_message(message_buf *buf) {
    if (buf->prev != NULL) {
        buf->prev->next = buf->next;
    }
    if (buf->next != NULL) {
        buf->next->prev = buf->prev;
    }
    free(buf->data);
    free(buf);
}

void free_message_buf(message_buf *buf) {
    if (buf->next != NULL) {
        free_message_buf(buf->next);
    }
    free(buf->data);
    free(buf);
}

message_buf *find_message(message_buf *buf, int process_id, int tag) {
    if (buf->process_id == process_id && buf->tag == tag) {
        return buf;
    } else if (buf->next != NULL) {
        return find_message(buf->next, process_id, tag);
    } else {
        return NULL;
    }
}

// read thread
void *read_thread(void *arg) {
    int process_id = *(int *)arg;
    int read_fd = fd_read(process_id, rank, size);
    // printf("Proces %d read fd from %d: %d\n", rank, process_id, read_fd);

    while (1) {
        // printf("Proces %d read thread %d wchodzi w petle\n", rank, process_id);
        int count = 0;
        int tag = 0;
        // read count
        int bytes_read = chrecv(read_fd, &count, sizeof(int));
        // printf("Proces %d read thread %d odczytal count: %d\n", rank, process_id , count);
        // printf("Proces %d read thread %d odczytal bytes_read: %d\n", rank, process_id, bytes_read);
        if (bytes_read == -1 || bytes_read == 0) {
            finished_processes[process_id] = true;
            // printf("Proces %d read thread konczy prace\n", rank);
            ASSERT_ZERO(pthread_mutex_lock(&mutex));
            ASSERT_ZERO(pthread_cond_signal(&cond));
            ASSERT_ZERO(pthread_mutex_unlock(&mutex));
            return NULL;
        }
        // read tag
        bytes_read = chrecv(read_fd, &tag, sizeof(int));
        if (bytes_read == -1 || bytes_read == 0) {
            finished_processes[process_id] = true;
            // printf("Proces %d read thread konczy prace\n", rank);
            return NULL;
        }

        // read data
        void *data = malloc(count);
        int data_index = 0;
        int count_left = count;
        while (count_left > 0) {
            int bytes_to_read = count_left > BUFFER_SIZE ? BUFFER_SIZE : count_left;
            int bytes_read = chrecv(read_fd, data + data_index, bytes_to_read);
            if (bytes_read == -1) {
                finished_processes[process_id] = true;
                // printf("Proces %d read thread konczy prace\n", rank);
                return NULL;
            }
            count_left -= bytes_read;
            data_index += bytes_read;
        }

        // printf("Proces %d read thread odczytal wiadomosc\n", rank);

        // get mutex
        ASSERT_ZERO(pthread_mutex_lock(&mutex));

        // add message to buffer
        message_buf *new_message = create_message_buf(process_id, tag, count, data);
        // printf("Proces %d read thread utworzyl wiadomosc:\n\t tag = %d, count "
        //    "= %d\n",
        //    rank, new_message->tag, new_message->count);
        add_message(message_buffer, new_message);

        // printf("Proces %d read thread dodal wiadomosc do bufora\n", rank);
        // printf("Proces %d read thread add message: %d\n", rank, message_buffer->next->tag);

        // signal main thread
        ASSERT_ZERO(pthread_cond_signal(&cond));
        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
    }
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    size = MIMPI_World_size();
    rank = MIMPI_World_rank();

    // printf("Proces %d inicjalizuje\n", rank);
    // init mutex and cond
    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&cond, NULL));

    // init finished processes
    finished_processes = calloc(size, sizeof(bool));

    // init message buffer
    message_buffer = malloc(sizeof(message_buf));
    init(message_buffer);

    // printf("Proces %d inicjalizuje watki\n", rank);
    // init read threads
    // TODO: use global table of args for threads and free it in finalize
    thread_args = malloc((size) * sizeof(int));
    read_threads = malloc((size) * sizeof(pthread_t));
    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        thread_args[i] = i;
        pthread_create(&read_threads[i], NULL, read_thread, &thread_args[i]);
    }
}

void MIMPI_Finalize() {
    // printf("\tProces %d wchodzi w finalize\n", rank);

    // Close all channels in use
    for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
            if (i == j) {
                continue;
            }
            if (i == rank) {
                // close read end
                // printf("Proces %d cloes read fd from %d to %d: %d\n", rank, j, i, fd_read(j, i, size));
                ASSERT_ZERO(close(fd_read(j, i, size)));
            } else if (j == rank) {
                // close write end
                // printf("Proces %d cloes write fd from %d to %d: %d\n", rank, j, i, fd_write(j, i, size));
                ASSERT_ZERO(close(fd_write(j, i, size)));
            }
        }
    }
    // printf("\tProces %d zamknał wszystkie kanaly\n", rank);

    // join read threads
    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        pthread_join(read_threads[i], NULL);
    }
    free(thread_args);

    // printf("\tProces %d join read threads\n", rank);

    // free message buffer
    free_message_buf(message_buffer);

    // free read threads
    free(read_threads);

    // free finished processes
    free(finished_processes);

    // destroy mutex and cond
    ASSERT_ZERO(pthread_mutex_destroy(&mutex));
    ASSERT_ZERO(pthread_cond_destroy(&cond));

    channels_finalize();
}

int MIMPI_World_size() { return atoi(getenv("MIMPI_WORLD_SIZE")); }

int MIMPI_World_rank() { return atoi(getenv("MIMPI_WORLD_RANK")); }

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination, int tag) {

    // printf("Proces %d send\n", rank);
    if (destination == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (destination < 0 || destination >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    if (finished_processes[destination]) {
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    int write_destination_fd = fd_write(rank, destination, size);
    // printf("Proces %d write fd to %d: %d\n", rank, destination, write_destination_fd);

    // count and tag for reading thread
    int *count_tag = malloc(2 * sizeof(int));
    count_tag[0] = count;
    count_tag[1] = tag;

    // send count_tag to reading thread
    int bytes_sent = chsend(write_destination_fd, count_tag, 2 * sizeof(int));
    free(count_tag);
    if (bytes_sent == -1) {
        finished_processes[destination] = true;
        return MIMPI_ERROR_REMOTE_FINISHED;
    }


    // send data to reading thread
    while (count > 0) {
        int bytes_to_send = count > BUFFER_SIZE ? BUFFER_SIZE : count;
        int bytes_sent = chsend(write_destination_fd, data, bytes_to_send);
        if (bytes_sent == -1) {
            finished_processes[destination] = true;
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        count -= bytes_sent;
        data += bytes_sent;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {

    // printf("Proces %d recv\n", rank);

    if (source == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (source < 0 || source >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    // get mutex
    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    // find message in buffer
    message_buf *message = find_message(message_buffer, source, tag);
    while (message == NULL && finished_processes[source] == false) {
        // wait for signal
        ASSERT_ZERO(pthread_cond_wait(&cond, &mutex));
        // find message in buffer
        message = find_message(message_buffer, source, tag);
    }

    if (message == NULL) {
        // release mutex
        // printf("Proces %d recv nie znalazl wiadomosci\n", rank);
        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    // copy data
    memcpy(data, message->data, message->count);

    // remove message from buffer
    remove_message(message);

    // release mutex
    ASSERT_ZERO(pthread_mutex_unlock(&mutex));

    // printf("Proces %d recv konczy\n", rank);

    return MIMPI_SUCCESS;
}

/*TODO:
W przypadku, jeśli synchronizacja wszystkich procesów nie może się zakończyć, bo któryś z procesów opuścił już blok MPI, wywołanie MIMPI_Barrier w przynajmniej jednym procesie powinno zakończyć się
kodem błędu MIMPI_ERROR_REMOTE_FINISHED. Jeśli proces, w którym się tak stanie w reakcji na błąd sam zakończy działanie, wywołanie MIMPI_Barrier powinno zakończyć się w przynajmniej jednym następnym
procesie. Powtarzając powyższe zachowanie, powinniśmy dojść do sytuacji, w której każdy proces opuścił barierę z błędem.
*/
MIMPI_Retcode MIMPI_Barrier() {
    // Send a message with a special tag to all processes.
    // The receive operation from each other program will block
    // until all other processes send a message with the same tag.

    // printf("Proces %d barrier\n", rank);
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        int ret = MIMPI_Send(NULL, 0, i, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            // printf("Proces %d barrier error\n", rank);
            return ret;
        }
    }

    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        int ret = MIMPI_Recv(NULL, 0, i, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            // printf("-- Proces %d barrier error from process %d\n", rank, i);
            return ret;
        }
    }

    // printf("Proces %d barrier konczy\n", rank);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root) {

    if (root < 0 || root >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    if (rank == root) {
        for (int i = 0; i < size; i++) {
            if (i == rank) {
                continue;
            }
            int ret = MIMPI_Send(data, count, i, BROADCAST_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }

        // wait untill all processes send message
        for (int i = 0; i < size; i++) {
            if (i == rank) {
                continue;
            }
            int ret = MIMPI_Recv(NULL, 0, i, BROADCAST_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }

    } else {
        int ret = MIMPI_Recv(data, count, root, BROADCAST_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
        // send message to root process
        ret = MIMPI_Send(NULL, 0, root, BROADCAST_TAG);
    }

    return MIMPI_SUCCESS;
}

/*
Zbiera dane zapewnione przez wszystkie procesy w send_data (traktując je jak tablicę liczb typu uint8_t wielkości count) i przeprowadza na elementach o tych samych indeksach z tablic send_data
wszystkich procesów (również root) redukcję typu op. Wynik redukcji, czyli tablica typu uint8_t wielkości count, jest zapisywany pod adres recv_data wyłącznie w procesie o randze root (niedozwolony
jest zapis pod adres recv_data w pozostałych procesach).
*/
void modify_data(u_int8_t *working_buffor, u_int8_t *data, int count, MIMPI_Op op) {
    switch (op) {
    case MIMPI_SUM: {
        for (int i = 0; i < count; i++) {
            working_buffor[i] += data[i];
        }
        break;
    }
    case MIMPI_PROD: {
        for (int i = 0; i < count; i++) {
            working_buffor[i] *= data[i];
        }
        break;
    }
    case MIMPI_MAX: {
        for (int i = 0; i < count; i++) {
            if (data[i] > working_buffor[i]) {
                working_buffor[i] = data[i];
            }
        }
        break;
    }
    case MIMPI_MIN: {
        for (int i = 0; i < count; i++) {
            if (data[i] < working_buffor[i]) {
                working_buffor[i] = data[i];
            }
        }
        break;
    }
    default:
        break;
    }
}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count, MIMPI_Op op, int root) {

    if (root < 0 || root >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    if (rank == root) {
        u_int8_t *working_buffor = malloc(count);
        memcpy(working_buffor, send_data, count);
        for (int i = 0; i < size; i++) {
            if (i == rank) {
                continue;
            }
            int ret = MIMPI_Recv(recv_data, count, i, REDUCTION_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
            modify_data(working_buffor, recv_data, count, op);
        }
        // send message to all processes
        for (int i = 0; i < size; i++) {
            if (i == rank) {
                continue;
            }
            int ret = MIMPI_Send(NULL, 0, i, REDUCTION_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }
        memcpy(recv_data, working_buffor, count);
        free(working_buffor);
    } else {
        int ret = MIMPI_Send(send_data, count, root, REDUCTION_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }

        // wait untill all processes send message
        // root will send message to all processes
        ret = MIMPI_Recv(NULL, 0, root, REDUCTION_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    return MIMPI_SUCCESS;
}