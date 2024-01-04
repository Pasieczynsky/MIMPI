/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <pthread.h>

#define BUFFER_SIZE 512

static int rank;
static pthread_mutex_t mutex;
static pthread_cond_t cond;


void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    rank = MIMPI_World_rank();

    ASSERT_ZERO(pthread_mutex_init(&mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&cond, NULL));
}

void MIMPI_Finalize() {

    ASSERT_ZERO(pthread_mutex_destroy(&mutex));
    ASSERT_ZERO(pthread_cond_destroy(&cond));

    channels_finalize();
}

int MIMPI_World_size() {
    return atoi(getenv("MIMPI_WORLD_SIZE"));
}

int MIMPI_World_rank() {
    return atoi(getenv("MIMPI_WORLD_RANK"));
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    if(destination == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if(destination < 0 || destination >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    int write_destination_fd = fd_number(rank, destination, size, 1);

    while(count > 0) {
        int bytes_to_write = count > BUFFER_SIZE ? BUFFER_SIZE : count;
        int bytes_written = write(write_destination_fd, data, bytes_to_write);
        if(bytes_written == -1) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        count -= bytes_written;
        data += bytes_written;
    }

    return MIMPI_SUCCESS;
}


MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    int rank = MIMPI_World_rank();
    int size = MIMPI_World_size();

    if(source == rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if(source < 0 || source >= size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }

    int read_source_fd = fd_number(source, rank, size, 0);

    // ASSERT_ZERO(pthread_mutex_lock(&mutex));
    // 
    // while(is_message_empty(read_source_fd, count, tag)) {
    //     ASSERT_ZERO(pthread_cond_wait(&cond, &mutex));
    // }

    while(count > 0) {
        int bytes_to_read = count > BUFFER_SIZE ? BUFFER_SIZE : count;
        int bytes_read = read(read_source_fd, data, bytes_to_read);
        if(bytes_read == -1) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        count -= bytes_read;
        data += bytes_read;
    }

    // ASSERT_ZERO(pthread_mutex_unlock(&mutex));

    return MIMPI_SUCCESS;    
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}