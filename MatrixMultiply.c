#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

typedef struct{
    int *matrix;
    int *vector;
    int *output;
    int row;
    int column;
} matrixMutliplyWork;

typedef struct{
    matrixMutliplyWork *fullWork;
    int start;
    int end;
} matrixMutliplyWorkChunk;

matrixMutliplyWork *initmatrixMutliplyWork()
{
    matrixMutliplyWork *work = (matrixMutliplyWork*)malloc(sizeof(matrixMutliplyWork)); 
    
    work->row = 0;
    work->column = 0;
    work->matrix = NULL;
    work->vector = NULL;
    work->output = NULL;

    return work;
}

void freematrixMutliplyWork(matrixMutliplyWork *work)
{
    if(work-> matrix != NULL)
    {
        free(work->matrix);
    }

    if(work->vector != NULL)
    {
        free(work->vector);
    }


    if(work->output != NULL)
    {
        free(work->output);
    }

    free(work);
}

void *multiply(void* arg)
{   
    matrixMutliplyWorkChunk  *workChunk = arg;
    int i, j;
    matrixMutliplyWork* work = workChunk->fullWork;
    printf("Start %d\t End %d\n", workChunk->start, workChunk->end);
    for (i = workChunk->start; i < workChunk->end; i++)
    {
        int sum = 0;
        for (j = 0; j < work->column; j++)
        {
            sum += work->matrix[i * work->column + j] * work->vector[j];
        }
        work->output[i] = sum;
    }
}

void printMatrix(matrixMutliplyWork *work)
{
    printf("row : %d, column : %d", work->row, work->column);
    int i, j;
    for (i = 0; i < work->row; i++)
    {
        for (j = 0; j < work->column; j++)
        {
                printf("%d\t", work->matrix[i * work->column + j]);
        }
        printf("\n");
    }

    for (i = 0; i < work->column; i++) 
    {
        printf("%d\n", work->vector[i]);
    }
}

void printOutput(int *array, int size)
{
    int i;
     for (i = 0; i < size; i++) 
    {
        printf("%d\n", array[i]);
    }
}

matrixMutliplyWork *read_inputFile(char *filename) {
	FILE *inputFile = fopen(filename, "r");

	if (!inputFile) {
		printf("Failed to open:");
		return NULL;
	}

	matrixMutliplyWork *work = initmatrixMutliplyWork();
    
	fscanf(inputFile, "%d,%d\n", &work->row, &work->column);
	work->matrix = (int*)malloc(sizeof(int) * work->row * work->column);

	int maxSizeOfLine = sizeof(char) * work->row * 11;
	char * buffer = (char *)malloc(maxSizeOfLine);
	char *record, *line;
	int i = 0, j = 0;

	for (i = 0; i < work->row; i++) {
		line = fgets(buffer, maxSizeOfLine, inputFile);
		j = 0;
		record = strtok(line, ",");
		while (record != NULL)
		{
			work->matrix[i * work->column + j] = atoi(record);
			j++;
			record = strtok(NULL, ",");
		}
	}

    work->vector = (int*)malloc(sizeof(int) * work->column);
    for (i = 0; i < work->column; i++) {
		line = fgets(buffer, maxSizeOfLine, inputFile);
		work->vector[i] = atoi(line);
	}

    work->output = (int*)malloc(sizeof(int) * work->row);


	free(buffer);
	fclose(inputFile);
	return work;
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
		printf("usage: %s inputFile\n", argv[0]);
		return -__LINE__;
	}

	matrixMutliplyWork *work = read_inputFile(argv[1]);

	if (work != NULL)
	{
		//printMatrix(work);

        int num_threads = 32;
        pthread_t   *threads;
        threads = malloc(sizeof(*threads) * num_threads);
        matrixMutliplyWorkChunk *workChunks = malloc(sizeof(matrixMutliplyWorkChunk) * num_threads);
        int chunk = work->row/num_threads;

        if(work->row % num_threads != 0)
        {
            chunk++;
        }

        printf("Chunk %d\n", chunk);
        int i,start = 0;

        for(i=0;i<num_threads;i++)
        {
            workChunks[i].fullWork = work;
            workChunks[i].start=start;

            if(start+chunk < work->row)
            {
                workChunks[i].end = start + chunk;
            }
            else
            {
                workChunks[i].end = work->row;
            }

            start = start+chunk;

            if (pthread_create(&threads[i], NULL, multiply, &workChunks[i]) != 0) 
            {
                perror("Failed to create thread\n");
            }
        }


        // Join threads
        for (i = 0; i < num_threads; i++) 
        {
            pthread_join(threads[i], NULL);
        }
        
        printOutput(work->output,work->row);
		freematrixMutliplyWork(work);
	}

	return 0;
}
