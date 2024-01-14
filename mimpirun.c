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

    // set MIMPI_WORLD_SIZE to the number of processes
    setenv("MIMPI_WORLD_SIZE", argv[1], 1);

    int channels[num_of_copies][num_of_copies][2];

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
                if (channels[i][j][0] != fd_read(i, j, num_of_copies)) {
                    // move channel to correct fd
                    ASSERT_SYS_OK(dup2(channels[i][j][0], fd_read(i, j, num_of_copies)));
                    // printf("dup2 %d to %d\n", channels[i][j][0], fd_read(i, j, num_of_copies));
                    // close old fd
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                }

                // write end
                if (channels[i][j][1] != fd_write(i, j, num_of_copies)) {
                    // move channel to correct fd
                    ASSERT_SYS_OK(dup2(channels[i][j][1], fd_write(i, j, num_of_copies)));
                    // printf("dup2 %d to %d\n", channels[i][j][1], fd_write(i, j, num_of_copies));
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
            // printf("----Child process %d\n", i);

            // close all channels that are not used by this process
            for (int k = 0; k < num_of_copies; k++) {
                for (int j = 0; j < num_of_copies; j++) {
                    if (k == j) {
                        continue;
                    }
                    if (k == i) {
                        // close read end
                        // printf("Proces %d cloes read fd from %d to %d: %d\n", i, k, j, fd_read(k, j, num_of_copies));
                        ASSERT_ZERO(close(fd_read(k, j, num_of_copies)));
                    } else if (j == i) {
                        // close write end
                        // printf("Proces %d cloes write fd from: %d to %d: %d\n", i, k, j, fd_write(k, j, num_of_copies));
                        ASSERT_ZERO(close(fd_write(k, j, num_of_copies)));
                    } else {
                        // close both ends
                        // printf("End both ends\n");
                        // printf("Proces %d cloes write fd from: %d to %d: %d\n", i, k, j, fd_write(k, j, num_of_copies));
                        ASSERT_ZERO(close(fd_write(k, j, num_of_copies)));
                        // printf("Proces %d cloes read fd from %d to %d: %d\n", i, k, j, fd_read(k, j, num_of_copies));
                        ASSERT_ZERO(close(fd_read(k, j, num_of_copies)));
                    }
                }
            }

            // printf("\n");
            // printf("Proces %d\nReads from fd: %d\nWrites to fd: %d\n", i, fd_read(1 - i, i, num_of_copies), fd_write(i, 1 - i, num_of_copies));
            // printf("\n");
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
                // printf("Closing %d and %d\n", fd_read(i, j, num_of_copies), fd_write(i, j, num_of_copies));
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