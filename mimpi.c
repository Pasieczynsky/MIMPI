/**
 * This file is for implementation of MIMPI library.
 * */

#include "mimpi.h"
#include "channel.h"
#include "mimpi_common.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define BUFFER_SIZE 512

#define BARRIER_TAG -1

static int rank;
static int size;

static pthread_mutex_t mutex;
static pthread_cond_t cond;
static pthread_t *read_threads;
message_buf *message_buffer;


//TODO: ogarnac te funkcje i zrobic je w
int fd_read(int writer, int reader, int size) {
    return 20 + reader * 2 + writer * size * 2;
}

int fd_write(int writer, int reader, int size) {
    return 20 + writer * 2 + reader * size * 2 + 1;
}

// buffor -> list of messages
typedef struct message_buf{
    int process_id;
    int tag;
    int count;
    void *data;
    struct message_buf *next;
    struct message_buf *prev;    
} message_buf;

void init(message_buf *buf){
    buf->process_id = -1;
    buf->tag = -1;
    buf->count = -1;
    buf->data = NULL;
    buf->next = NULL;
    buf->prev = NULL;
}

message_buf *create_message_buf(int process_id, int tag, int count, void *data){
    message_buf *buf = malloc(sizeof(message_buf));
    buf->process_id = process_id;
    buf->tag = tag;
    buf->count = count;
    buf->data = data;
    buf->next = NULL;
    buf->prev = NULL;
    return buf;
}

void add_message(message_buf *buf, message_buf *new_message){
    if(buf->next == NULL){
        buf->next = new_message;
        new_message->prev = buf;
    } else {
        add_message(buf->next, new_message);
    }
}

void remove_message(message_buf *buf){
    if(buf->prev != NULL){
        buf->prev->next = buf->next;
    }
    if(buf->next != NULL){
        buf->next->prev = buf->prev;
    }
    free(buf);
}

void free_message_buf(message_buf *buf){
    if(buf->next != NULL){
        free_message_buf(buf->next);
    }
    free(buf);
}

message_buf *find_message(message_buf *buf, int process_id, int tag){
    if(buf->process_id == process_id && buf->tag == tag){
        // TODO: remove message from list
        // remove_message(buf);
        return buf;
    } else if(buf->next != NULL){
        return find_message(buf->next, process_id, tag);
    } else {
        return NULL;
    }
}


// read thread
void *read_thread(void *arg) {
    int process_id = (int)arg;
    int read_fd = fd_read(process_id, rank, size);
    printf("Proces %d read fd from %d: %d\n", rank, process_id, read_fd);

    while (1) {
        int count = 0;
        int tag = 0;
        // read count
        int bytes_read = chrecv(read_fd, &count, sizeof(int));
        if (bytes_read == -1) {
            printf("MIMPI_ERROR_REMOTE_FINISHED\n");
            return NULL;
        }
        // read tag
        bytes_read = chrecv(read_fd, &tag, sizeof(int));
        if (bytes_read == -1) {
            printf("MIMPI_ERROR_REMOTE_FINISHED\n");
            return NULL;
        }

        // read data
        void *data = malloc(count);
        while (count > 0) {
            int bytes_to_read = count > BUFFER_SIZE ? BUFFER_SIZE : count;
            int bytes_read = chrecv(read_fd, data, bytes_to_read);
            if (bytes_read == -1) {
                printf("MIMPI_ERROR_REMOTE_FINISHED\n");
                return NULL;
            }
            count -= bytes_read;
            data += bytes_read;
        }

        // get mutex
        ASSERT_ZERO(pthread_mutex_lock(&mutex));

        // add message to buffer
        message_buf *new_message = create_message_buf(process_id, tag, count, data);
        add_message(message_buffer, new_message);

        // signal main thread
        ASSERT_ZERO(pthread_cond_signal(&cond));
        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
    }
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    size = MIMPI_World_size();
    rank = MIMPI_World_rank();

    // Close all channels not in use
    for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
            if (i == j) {
                continue;
            }
            if (i == rank) {
                // close write end
                // printf("Proces %d cloes write fd to %d: %d\n", i, j, fd_write(i, j, size));
                ASSERT_ZERO(close(fd_write(i, j, size)));
                // printf("Proces %d read fd from %d: %d\n", i, j, fd_read(i, j,
                // size));

            } else if (j == rank) {
                // close read end
                // printf("Proces %d cloes read fd from %d: %d\n", i, j, fd_read(i, j, size));
                ASSERT_ZERO(close(fd_read(i, j, size)));
                // printf("Proces %d write fd to %d: %d\n", i, j, fd_write(i, j,
                // size));

            } else {
                // close both ends
                // printf("Proces %d cloes write fd from: %d to %d: %d\n", rank, i, j, fd_write(i, j, size));
                ASSERT_ZERO(close(fd_write(i, j, size)));
                // printf("Proces %d cloes read fd from %d to %d: %d\n", rank, i, j, fd_read(i, j, size));
                ASSERT_ZERO(close(fd_read(i, j, size)));
            }
        }
    }

    // init mutex and cond
    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&cond, NULL));

    // init message buffer
    message_buffer = malloc(sizeof(message_buf));
    init(message_buffer);

    // init read threads
    read_threads = malloc((size) * sizeof(pthread_t));
    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        pthread_create(&read_threads[i], NULL, read_thread, (void *)i);
    }    
}

void MIMPI_Finalize() {

    // ASSERT_ZERO(pthread_mutex_destroy(&mutex));
    // ASSERT_ZERO(pthread_cond_destroy(&cond));

    // Close all channels in use
    for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
            if (i == j) {
                continue;
            }
            if (i == rank) {
                // close read end
                // printf("Proces %d cloes read fd from %d to %d: %d\n", rank, i, j, fd_read(i, j, size));
                ASSERT_ZERO(close(fd_read(i, j, size)));
            } else if (j == rank) {
                // close write end
                // printf("Proces %d cloes write fd from %d to %d: %d\n", rank, i, j, fd_write(i, j, size));
                ASSERT_ZERO(close(fd_write(i, j, size)));
            }
        }
    }

    // join read threads
    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        pthread_join(read_threads[i], NULL);
    }

    // free message buffer
    free_message_buf(message_buffer);

    // free read threads
    free(read_threads);
    
    // destroy mutex and cond
    ASSERT_ZERO(pthread_mutex_destroy(&mutex));
    ASSERT_ZERO(pthread_cond_destroy(&cond));

    channels_finalize();
}

int MIMPI_World_size() { return atoi(getenv("MIMPI_WORLD_SIZE")); }

int MIMPI_World_rank() { return atoi(getenv("MIMPI_WORLD_RANK")); }

MIMPI_Retcode MIMPI_Send(void const *data, int count, int destination,
                         int tag) {

    if (destination == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (destination < 0 || destination >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    int write_destination_fd = fd_write(destination, rank, size);
    printf("Proces %d write fd to %d: %d\n", rank, destination, write_destination_fd);

    // count and tag for reading thread
    int *count_tag = malloc(2 * sizeof(int));
    count_tag[0] = count;
    count_tag[1] = tag;

    // send count_tag to reading thread
    int bytes_sent = chsend(write_destination_fd, count_tag, 2 * sizeof(int));
    if (bytes_sent == -1) {
        printf("MIMPI_ERROR_REMOTE_FINISHED\n");
        return MIMPI_ERROR_REMOTE_FINISHED;
    }


    // send data to reading thread
    while (count > 0) {
        int bytes_to_send = count > BUFFER_SIZE ? BUFFER_SIZE : count;
        int bytes_sent = chsend(write_destination_fd, data, bytes_to_send);
        if (bytes_sent == -1) {
            printf("MIMPI_ERROR_REMOTE_FINISHED\n");
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        count -= bytes_sent;
        data += bytes_sent;
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(void *data, int count, int source, int tag) {

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
    while (message == NULL) {
        // wait for signal
        ASSERT_ZERO(pthread_cond_wait(&cond, &mutex));
        // find message in buffer
        message = find_message(message_buffer, source, tag);
    }

    // copy data
    memcpy(data, message->data, message->count);

    // remove message from buffer
    remove_message(message);

    // release mutex
    ASSERT_ZERO(pthread_mutex_unlock(&mutex));

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    // Send a message with a special tag to all processes.
    // The receive operation from each other program will block
    // until all other processes send a message with the same tag.

    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        int ret = MIMPI_Send(NULL, 0, i, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    for (int i = 0; i < size; i++) {
        if (i == rank) {
            continue;
        }
        int ret = MIMPI_Recv(NULL, 0, i, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root){TODO}

MIMPI_Retcode MIMPI_Reduce(void const *send_data, void *recv_data, int count,
                           MIMPI_Op op, int root) {
    TODO
}