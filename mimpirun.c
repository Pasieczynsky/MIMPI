/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

// i - beginning process
// j - ending process
// num_of_copies - number of processes
// is_read_end - 0 for read end, 1 for write end
int fd_number(int i, int j, int num_of_copies, int is_write_end) {
    return 20 + j * 2 + i * num_of_copies * 2 + is_write_end;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fatal("Not enough arguments");
    }

    int num_of_copies = atoi(argv[1]);
    char *program_path = argv[2];
    char **program_args = &argv[2];

    // Prepare environment

    // set MIMPI_WORLD_SIZE to the number of processes
    setenv("MIMPI_WORLD_SIZE", argv[1], 1);

    // create channels to communicate
    // use functions from channel.h
    // every process should have a separate pair of channels
    // for every other process
    int channels[num_of_copies][num_of_copies][2];

    // chanels[x][y] is a pipe to communicate with process y
    // chanles[x][y][0] is a read end
    // chanels[x][y][1] is a write end

    // create chanels
    // fds only from 20 to 1023 are available
    for (int i = 0; i < num_of_copies; i++) {
        for (int j = 0; j < num_of_copies; j++) {
            if (i != j) {
                // create channel
                ASSERT_SYS_OK(pipe(channels[i][j]));

                // check if channel is created correctly
                // read end
                if (channels[i][j][0] != fd_number(i, j, num_of_copies, 0)) {
                    // move channel to correct fd
                    ASSERT_SYS_OK(dup2(channels[i][j][0],
                                       fd_number(i, j, num_of_copies, 0)));
                    // close old fd
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                }
                // write end
                if (channels[i][j][1] != fd_number(i, j, num_of_copies, 1)) {
                    // move channel to correct fd
                    ASSERT_SYS_OK(dup2(channels[i][j][1],
                                       fd_number(i, j, num_of_copies, 1)));
                    // close old fd
                    ASSERT_SYS_OK(close(channels[i][j][1]));
                }
            }
        }
    }

    // Run n copies of program prog, each in a separate process
    for (int i = 0; i < num_of_copies; i++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (!pid) {
            // Child process
            // set MIMPI_WORLD_RANK to the rank of the process
            char buffer[3];
            int ret = snprintf(buffer, sizeof buffer, "%d", i);
            if (ret < 0 || ret >= (int)sizeof(buffer))
                fatal("snprintf failed");

            ASSERT_SYS_OK(setenv("MIMPI_WORLD_RANK", buffer, 1));
            ASSERT_SYS_OK(execvp(program_path, program_args));
        }
    }

    // Wait for all created processes to finish
    for (int i = 0; i < num_of_copies; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }

    // Close all channels
    for(int i = 0; i < num_of_copies; i++) {
        for(int j = 0; j < num_of_copies; j++) {
            if(i != j) {
                ASSERT_SYS_OK(close(fd_number(i, j, num_of_copies, 0)));
                ASSERT_SYS_OK(close(fd_number(i, j, num_of_copies, 1)));
            }
        }
    } 

    // Finish
    return 0;
}