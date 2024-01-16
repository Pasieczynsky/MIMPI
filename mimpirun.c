/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fatal("Not enough arguments");
    }

    int num_of_copies = atoi(argv[1]);
    char *program_path = argv[2];
    char **program_args = &argv[2];

    // Prepare environment

    setenv("MIMPI_WORLD_SIZE", argv[1], 1);

    int channels[2];


    // create chanels
    // fds only from 20 to 1023 are available
    for (int i = 0; i < num_of_copies; i++) {
        for (int j = 0; j < num_of_copies; j++) {
            if (i != j) {
                // create channel
                ASSERT_SYS_OK(pipe(channels));

                // check if channel is created correctly
                // read end
                if (channels[0] != fd_read(i, j, num_of_copies)) {
                    // move channel to correct fd
                    ASSERT_SYS_OK(dup2(channels[0], fd_read(i, j, num_of_copies)));
                    // close old fd
                    ASSERT_SYS_OK(close(channels[0]));
                }

                // write end
                if (channels[1] != fd_write(i, j, num_of_copies)) {
                    // move channel to correct fd
                    ASSERT_SYS_OK(dup2(channels[1], fd_write(i, j, num_of_copies)));
                    // close old fd
                    ASSERT_SYS_OK(close(channels[1]));
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

            // close all channels that are not used by this process
            for (int k = 0; k < num_of_copies; k++) {
                for (int j = 0; j < num_of_copies; j++) {
                    if (k == j) {
                        continue;
                    }
                    if (k == i) {
                        // close read end
                        ASSERT_ZERO(close(fd_read(k, j, num_of_copies)));
                    } else if (j == i) {
                        // close write end
                        ASSERT_ZERO(close(fd_write(k, j, num_of_copies)));
                    } else {
                        // close both ends
                        ASSERT_ZERO(close(fd_write(k, j, num_of_copies)));
                        ASSERT_ZERO(close(fd_read(k, j, num_of_copies)));
                    }
                }
            }

            // set MIMPI_WORLD_RANK to the rank of the process
            char buffer[3];
            int ret = snprintf(buffer, sizeof buffer, "%d", i);
            if (ret < 0 || ret >= (int)sizeof(buffer))
                fatal("snprintf failed");

            ASSERT_SYS_OK(setenv("MIMPI_WORLD_RANK", buffer, 1));
            ASSERT_SYS_OK(execvp(program_path, program_args));
        }
    }
    // Close all channels
    for (int i = 0; i < num_of_copies; i++) {
        for (int j = 0; j < num_of_copies; j++) {
            if (i != j) {
                ASSERT_SYS_OK(close(fd_read(i, j, num_of_copies)));
                ASSERT_SYS_OK(close(fd_write(i, j, num_of_copies)));
            }
        }
    }

    // Wait for all created processes to finish
    for (int i = 0; i < num_of_copies; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }

    // Finish
    return 0;
}