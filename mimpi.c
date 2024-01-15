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

#define BARRIER_TAG -2
#define BROADCAST_TAG -3
#define REDUCTION_TAG -4

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

message_buf *find_message(message_buf *buf, int process_id, int tag, int count) {
    if (buf->next == NULL) {
        return NULL;
    }
    if (buf->next->process_id == process_id && (buf->next->tag == tag || tag == 0) && buf->next->count == count) {
        return buf->next;
    }
    return find_message(buf->next, process_id, tag, count);
}

void *read_thread(void *arg) {
    int process_id = *(int *)arg;
    int read_fd = fd_read(process_id, rank, size);

    while (1) {
        int count = 0;
        int tag = 0;
        // read count
        int bytes_read = chrecv(read_fd, &count, sizeof(int));
        if (bytes_read == -1 || bytes_read == 0) {
            finished_processes[process_id] = true;
            ASSERT_ZERO(pthread_mutex_lock(&mutex));
            ASSERT_ZERO(pthread_cond_signal(&cond));
            ASSERT_ZERO(pthread_mutex_unlock(&mutex));
            return NULL;
        }
        // read tag
        bytes_read = chrecv(read_fd, &tag, sizeof(int));
        if (bytes_read == -1 || bytes_read == 0) {
            finished_processes[process_id] = true;
            ASSERT_ZERO(pthread_mutex_lock(&mutex));
            ASSERT_ZERO(pthread_cond_signal(&cond));
            ASSERT_ZERO(pthread_mutex_unlock(&mutex));
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
                return NULL;
            }
            count_left -= bytes_read;
            data_index += bytes_read;
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

    // init mutex and cond
    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&cond, NULL));

    // init finished processes
    finished_processes = calloc(size, sizeof(bool));

    // init message buffer
    message_buffer = malloc(sizeof(message_buf));
    init(message_buffer);

    // init read threads
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
    // Close all channels in use
    for (int i = 0; i < size; i++) {
        for (int j = 0; j < size; j++) {
            if (i == j) {
                continue;
            }
            if (i == rank) {
                // close read end
                ASSERT_ZERO(close(fd_read(j, i, size)));
            } else if (j == rank) {
                // close write end
                ASSERT_ZERO(close(fd_write(j, i, size)));
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
    free(thread_args);


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

    if (source == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if (source < 0 || source >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    // get mutex
    ASSERT_ZERO(pthread_mutex_lock(&mutex));

    // find message in buffer
    message_buf *message = find_message(message_buffer, source, tag, count);
    while (message == NULL && finished_processes[source] == false) {
        ASSERT_ZERO(pthread_cond_wait(&cond, &mutex));
        // find message in buffer
        message = find_message(message_buffer, source, tag, count);
    }

    if (message == NULL) {
        // release mutex
        ASSERT_ZERO(pthread_mutex_unlock(&mutex));
        return MIMPI_ERROR_REMOTE_FINISHED;
    }

    // copy data
    memcpy(data, message->data, message->count);

    // remove message from buffer
    remove_message(message);

    // release mutex
    ASSERT_ZERO(pthread_mutex_unlock(&mutex));

    return MIMPI_SUCCESS;
}

int left_son(int rank, int root) {
    if (root == 0) {
        return rank * 2 + 1;
    }
    int table_index = (rank - root + size) % size;
    int left_son_index = 2 * table_index + 1;
    if (left_son_index >= size) {
        return size;
    }
    return (left_son_index + root) % size;
}

int right_son(int rank, int root) {
    if (root == 0) {
        return rank * 2 + 2;
    }
    int table_index = (rank - root + size) % size;
    int right_son_index = 2 * table_index + 2;
    if (right_son_index >= size) {
        return size;
    }
    return (right_son_index + root) % size;
}

int parent(int rank, int root) {
    if (root == 0) {
        return (rank - 1) / 2;
    }
    if (rank == root) {
        return rank;
    }
    int table_index = (rank - root + size) % size;
    int parent_index = (table_index - 1) / 2;
    return (parent_index + root) % size;
}


MIMPI_Retcode MIMPI_Barrier() {

    int ret = MIMPI_SUCCESS;
    int left_son_rank = rank * 2 + 1;
    int right_son_rank = rank * 2 + 2;

    if (left_son_rank < size) {
        ret = MIMPI_Recv(NULL, 0, left_son_rank, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
        if (right_son_rank < size) {
            ret = MIMPI_Recv(NULL, 0, right_son_rank, BARRIER_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }
    }

    if (rank != 0) {
        int parent_rank = (rank - 1) / 2;
        // send message to parent
        ret = MIMPI_Send(NULL, 0, parent_rank, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }

        // wait for message from parent
        ret = MIMPI_Recv(NULL, 0, parent_rank, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    // send message to two sons if thet exist
    if (left_son_rank < size) {
        ret = MIMPI_Send(NULL, 0, left_son_rank, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
        if (right_son_rank < size) {
            ret = MIMPI_Send(NULL, 0, right_son_rank, BARRIER_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }
    }
    return ret;
}

MIMPI_Retcode MIMPI_Bcast(void *data, int count, int root) {
    if (root < 0 || root >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    int ret = MIMPI_SUCCESS;
    int left_son_rank = left_son(rank, root);
    int right_son_rank = right_son(rank, root);

    // receive data from parent
    if (rank != root) {
        ret = MIMPI_Recv(data, count, parent(rank, root), BROADCAST_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    // send data to sons
    if (left_son_rank < size) {
        ret = MIMPI_Send(data, count, left_son_rank, BROADCAST_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
        if (right_son_rank < size) {
            ret = MIMPI_Send(data, count, right_son_rank, BROADCAST_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }
    }

    // wait for messages from nodes
    if (rank == root) {
        int count_of_nodes = (size - 1) / 2 + 1;
        int leaf = 0;
        for (int i = 0; i < count_of_nodes; i++) {

            leaf = (rank - i + size - 1) % size;

            ret = MIMPI_Recv(NULL, 0, leaf, BARRIER_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }
    }
    // only nodes send message to root
    if (rank != root && left_son_rank >= size && right_son_rank >= size) {
        ret = MIMPI_Send(NULL, 0, root, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    return ret;
}

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

    int ret = MIMPI_SUCCESS;
    int left_son_rank = left_son(rank, root);
    int right_son_rank = right_son(rank, root);

    u_int8_t *working_buffor = malloc(count);
    memcpy(working_buffor, send_data, count);

    // receive data from children
    if (left_son_rank < size) {
        u_int8_t *data = malloc(count);
        ret = MIMPI_Recv(data, count, left_son_rank, REDUCTION_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
        modify_data(working_buffor, data, count, op);
        free(data);
        if (right_son_rank < size) {
            data = malloc(count);
            ret = MIMPI_Recv(data, count, right_son_rank, REDUCTION_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
            modify_data(working_buffor, data, count, op);
            free(data);
        }
    }

    // send data to parent
    if (rank != root) {
        ret = MIMPI_Send(working_buffor, count, parent(rank, root), REDUCTION_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    } else {
        memcpy(recv_data, working_buffor, count);
    }

    // wait for message from parent
    if (rank != root) {
        ret = MIMPI_Recv(NULL, 0, parent(rank, root), BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
    }

    // send message to children
    if (left_son_rank < size) {
        ret = MIMPI_Send(NULL, 0, left_son_rank, BARRIER_TAG);
        if (ret != MIMPI_SUCCESS) {
            return ret;
        }
        if (right_son_rank < size) {
            ret = MIMPI_Send(NULL, 0, right_son_rank, BARRIER_TAG);
            if (ret != MIMPI_SUCCESS) {
                return ret;
            }
        }
    }

    free(working_buffor);
    return MIMPI_SUCCESS;
}