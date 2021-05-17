#include "mpi.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MASTER 0
#define HORROR 1
#define COMEDY 2
#define FANTASY 3
#define SCIFI 4
#define HORROR_TEXT_TAG 1
#define HORROR_CONTROL_TAG 2
#define COMEDY_TEXT_TAG 3
#define COMEDY_CONTROL_TAG 4
#define FANTASY_TEXT_TAG 5
#define FANTASY_CONTROL_TAG 6
#define SCIFI_TEXT_TAG 7
#define SCIFI_CONTROL_TAG 8
#define READING_THREADS 4

FILE *out_file;
char file[500];
int sizeToSend = 1000000;

pthread_mutex_t mutex;

void *reading_threads(void *arg) {
    int tid = *(int *)arg;
    MPI_Status status;
    int runningWorkersControl = 1;

    char genre_reader[150000];
    char reader[150000];
    char genre[4][20] = {"horror", "comedy", "fantasy", "science-fiction"};

    char *paragraph_horror;
    char *paragraph_comedy;
    char* paragraph_fantasy;
    char* paragraph_scifi;

    FILE *in_file;
    if ((in_file = fopen(file, "r")) == NULL) {
            printf("Error! opening input file");
            exit(1);
    }

// CITIRE FISIER INPUT
    while (fgets(genre_reader, sizeof(genre_reader), in_file) != NULL) {

// HORROR
        if (tid == 0 && !strncmp(genre_reader, genre[0], 6)) {
        
            paragraph_horror = (char *)malloc(sizeToSend);
            *paragraph_horror = '\0';

            while (fgets(reader, sizeof(reader), in_file) != NULL) {
                strcat(paragraph_horror, reader);
                if (strlen(reader) == 1 && !strncmp(reader, "\n", 1)) {
                    break;
                }
            }

            // Trimitere paragraf la worker-ul horror
            pthread_mutex_lock(&mutex);
                MPI_Send(&runningWorkersControl, 1, MPI_INT, HORROR, HORROR_CONTROL_TAG , MPI_COMM_WORLD);
                MPI_Send(paragraph_horror, sizeToSend, MPI_CHAR, HORROR, HORROR_TEXT_TAG, MPI_COMM_WORLD);
            pthread_mutex_unlock(&mutex);
            free(paragraph_horror);
        }

// COMEDY
        if (tid == 1 && !strncmp(genre_reader, genre[1], 6)) {

            paragraph_comedy = (char *)malloc(sizeToSend);
            *paragraph_comedy = '\0';

            while (fgets(reader, sizeof(reader), in_file) != NULL) {
                strcat(paragraph_comedy, reader);
                if (strlen(reader) == 1 && !strncmp(reader, "\n", 1)) {
                    break;
                }
            }

            // Trimitere paragraf la worker-ul comedy
            pthread_mutex_lock(&mutex);
                MPI_Send(&runningWorkersControl, 1, MPI_INT, COMEDY, COMEDY_CONTROL_TAG, MPI_COMM_WORLD);
                MPI_Send(paragraph_comedy, sizeToSend, MPI_CHAR, COMEDY, COMEDY_TEXT_TAG, MPI_COMM_WORLD);
            pthread_mutex_unlock(&mutex);
            free(paragraph_comedy);
        } 

// FANTASY
        if (tid == 2 && !strncmp(genre_reader, genre[2], 7)) {
            paragraph_fantasy = (char *)malloc(sizeToSend);
            *paragraph_fantasy = '\0';

            while (fgets(reader, sizeof(reader), in_file) != NULL) {
                strcat(paragraph_fantasy, reader);
                if (strlen(reader) == 1 && !strncmp(reader, "\n", 1)) {
                    break;
                }
            }

            // Trimitere paragraf la worker-ul fantasy
            pthread_mutex_lock(&mutex); 
                MPI_Send(&runningWorkersControl, 1, MPI_INT, FANTASY, FANTASY_CONTROL_TAG, MPI_COMM_WORLD);
                MPI_Send(paragraph_fantasy, sizeToSend, MPI_CHAR, FANTASY, FANTASY_TEXT_TAG, MPI_COMM_WORLD);
            pthread_mutex_unlock(&mutex);
            free(paragraph_fantasy);
        } 

// SCI-FI
        if (tid == 3 && !strncmp(genre_reader, genre[3], 15)) {

            paragraph_scifi = (char *)malloc(sizeToSend);
            *paragraph_scifi = '\0';

            while (fgets(reader, sizeof(reader), in_file) != NULL) {
                strcat(paragraph_scifi, reader);
                if (strlen(reader) == 1 && !strncmp(reader, "\n", 1)) {
                    break;
                }
            }

            // Trimitere paragraf la worker-ul sci-fi
            pthread_mutex_lock(&mutex);
                MPI_Send(&runningWorkersControl, 1, MPI_INT, SCIFI, SCIFI_CONTROL_TAG, MPI_COMM_WORLD);
                MPI_Send(paragraph_scifi, sizeToSend, MPI_CHAR, SCIFI, SCIFI_TEXT_TAG, MPI_COMM_WORLD);
            pthread_mutex_unlock(&mutex);
            free(paragraph_scifi);
        } 

    }
    pthread_exit(NULL);
    
}

// Citire cale catre fisierul de intrare
void get_args(int argc, char **argv, char *file)
{
	if(argc < 1) {
		printf("Numar insuficient de parametri");
		exit(1);
	}

    strcpy(file, argv[1]);
}



int main (int argc, char *argv[])
{
    int rank;
    int nProcesses;
    MPI_Init(&argc, &argv);
    int runningWorkersControl = 1;
    MPI_Status status;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    if (rank == MASTER) {

        // DEFINIRE OUTPUT_FILE
        char output[50];
        get_args(argc, argv, file);
        strncpy(output, file, 17);
        output[17] = '\0';
        strcat(output, ".out\0");
        if ((out_file = fopen(output, "w")) == NULL) {
            printf("Error! opening output file\n");
            exit(1);
        }

        int i;
        int thread_id[READING_THREADS];
        pthread_t tid[READING_THREADS];

// Deschidere thread-uri
        pthread_mutex_init(&mutex, NULL);
        for (i = 0; i < READING_THREADS; i++) {
		    thread_id[i] = i;
		    pthread_create(&tid[i], NULL, reading_threads, &thread_id[i]);
	    }

// Citire pentru a determina ordinea de afisare
        int orderToPrint[1000]; // 1 = horror, 2 - comedy, 3 - fantasy, 4 - scifi
        int numberOfParagraphs = 0;
        char genre_reader[1500];
        char reader[1500];
        char genre[4][20] = {"horror", "comedy", "fantasy", "science-fiction"};

        FILE *in_file;
        if ((in_file = fopen(file, "r")) == NULL) {
                printf("Error! opening input file");
                exit(1);
        }

        while (fgets(genre_reader, sizeof(genre_reader), in_file) != NULL) {
            if (!strncmp(genre_reader, genre[0], 6)) {
                orderToPrint[numberOfParagraphs] = 1;
                numberOfParagraphs++;
            }
            if (!strncmp(genre_reader, genre[1], 6)) {
                orderToPrint[numberOfParagraphs] = 2;
                numberOfParagraphs++;
            }
            if (!strncmp(genre_reader, genre[2], 7)) {
                orderToPrint[numberOfParagraphs] = 3;
                numberOfParagraphs++;
            }
            if (!strncmp(genre_reader, genre[3], 15)) {
                orderToPrint[numberOfParagraphs] = 4;
                numberOfParagraphs++;
            }

        }

// Se asteapta thread-urile
	    for (i = 0; i < READING_THREADS; i++) {
		    pthread_join(tid[i], NULL);
    	}

        pthread_mutex_destroy(&mutex);

// Oprire workeri dupa ce tot textul a fost citit
        runningWorkersControl = 0;
        MPI_Send(&runningWorkersControl, 1, MPI_INT, COMEDY, COMEDY_CONTROL_TAG, MPI_COMM_WORLD);
        MPI_Send(&runningWorkersControl, 1, MPI_INT, HORROR, HORROR_CONTROL_TAG, MPI_COMM_WORLD);
        MPI_Send(&runningWorkersControl, 1, MPI_INT, FANTASY, FANTASY_CONTROL_TAG, MPI_COMM_WORLD);
        MPI_Send(&runningWorkersControl, 1, MPI_INT, SCIFI, SCIFI_CONTROL_TAG, MPI_COMM_WORLD);

// Afisare dupa ordinea definita in MASTER -- receiv-uri succesive in functie de ordinea aparitiei paragrafelor
        char *paragraph_out;
        for (int i = 0; i < numberOfParagraphs; i++) {
            paragraph_out = (char *)malloc(sizeToSend);
            if (orderToPrint[i] == HORROR) {
                MPI_Recv(paragraph_out, sizeToSend, MPI_CHAR, HORROR, HORROR_TEXT_TAG, MPI_COMM_WORLD, &status);
                fprintf(out_file, "%s", "horror\n");
            } else if (orderToPrint[i] == COMEDY) {
               MPI_Recv(paragraph_out, sizeToSend, MPI_CHAR, COMEDY, COMEDY_TEXT_TAG, MPI_COMM_WORLD, &status);
                fprintf(out_file, "%s", "comedy\n");
            } else if (orderToPrint[i] == FANTASY) {
                MPI_Recv(paragraph_out, sizeToSend, MPI_CHAR, FANTASY, FANTASY_TEXT_TAG, MPI_COMM_WORLD, &status);
                fprintf(out_file, "%s", "fantasy\n");
            } else if (orderToPrint[i] == SCIFI) {
                MPI_Recv(paragraph_out, sizeToSend, MPI_CHAR, SCIFI, SCIFI_TEXT_TAG, MPI_COMM_WORLD, &status);
                fprintf(out_file, "%s", "science-fiction\n");
            }
            fprintf(out_file,"%s", paragraph_out);
            free(paragraph_out);
        }

    }


// WORKER HORROR
    if (rank == HORROR) {
        char *paragraph;
        char *paragraphAux;

        char *horror_text;
        int horrorParagraphs;
        
        horrorParagraphs = 0;
        horror_text = (char *)malloc(sizeToSend);

        // Receive pentru controlul worker-ului
        MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, HORROR_CONTROL_TAG, MPI_COMM_WORLD, &status);

        // PROCESARE TEXT
        while(runningWorkersControl) {
            paragraph = (char *)malloc(sizeToSend);
            paragraphAux = (char *)malloc(sizeToSend);
            strcpy(paragraph, "\0");
            strcpy(paragraphAux, "\0");

            // Receive la paragraph-ul trimis de thread-ul horror din master
            MPI_Recv(paragraph, sizeToSend, MPI_CHAR, MASTER, HORROR_TEXT_TAG, MPI_COMM_WORLD, &status);

            // Procesare text horror
            int j = 0;
            for (int i = 0; i < strlen(paragraph); i++) {
                paragraphAux[j] = paragraph[i];
                j++;
                if (!strchr("aeiouAEIOU \n,.';", paragraph[i])) { 
                    if (paragraph[i] >= 65 && paragraph[i] <= 90) {
                        paragraphAux[j] = paragraph[i] + 32;
                    } else {
                    paragraphAux[j] = paragraph[i];
                    }
                    j++;
                }
            }

            // Stocare paragraf procesat
            horrorParagraphs++;
            horror_text = (char *)realloc(horror_text, horrorParagraphs *  sizeToSend);
            strcat(horror_text, paragraphAux);
            
            free(paragraph);
            free(paragraphAux);

            // Receive pentru controlul worker-ului
            MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, HORROR_CONTROL_TAG, MPI_COMM_WORLD, &status);
            if (runningWorkersControl == 0) {
                break;
            }
        }  

        // Trimitere cate un paragraf pe rand catre MASTER pentru afisare
        if (horrorParagraphs != 0) {
            int k = 0;
            for (int i = 0; i < horrorParagraphs; i++) {
                char paragraphToSend[sizeToSend];
                int j = 0;
                while(1) {
                    paragraphToSend[j] = horror_text[k];
                    j++;
                    k++;
                    if ( (horror_text[k] == '\n' && horror_text[k - 1] == '\n') || horror_text[k] == '\0') {
                        paragraphToSend[j] = '\n';
                        j++;
                        k++;
                        break;
                    }
                }
                paragraphToSend[j] = '\0';
                MPI_Send(paragraphToSend, sizeToSend, MPI_CHAR, MASTER, HORROR_TEXT_TAG, MPI_COMM_WORLD);
            }
        }
    }


// WORKER COMEDY
    if (rank == COMEDY) {
        char *paragraph;
        char *comedy_text;
        int comedyParagraphs;

        comedy_text = (char *)malloc(sizeToSend);
        comedyParagraphs = 0;

        // Receive pentru controlul worker-ului
        MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, COMEDY_CONTROL_TAG, MPI_COMM_WORLD, &status);

        // PROCESARE TEXT
        while(runningWorkersControl) {
            paragraph = (char *)malloc(sizeToSend);
            strcpy(paragraph, "\0");

            // Receive la paragraph-ul trimis de thread-ul comedy din master
            MPI_Recv(paragraph, sizeToSend, MPI_CHAR, MASTER, COMEDY_TEXT_TAG, MPI_COMM_WORLD, &status);

            // Procesare text comedy
            int numberOfChars = 1;
            for (int i = 0; i < strlen(paragraph); i++) {
                if (paragraph[i] == ' ' || paragraph[i] == '\n') {
                    numberOfChars = 1;
                } else if (numberOfChars % 2 == 0 && numberOfChars != 0) {
                    if (paragraph[i] >= 97 && paragraph[i] <= 122) {
                        paragraph[i] = paragraph[i] - 32;
                        numberOfChars++;
                    } else {
                        numberOfChars++;
                    }
                } else {
                    numberOfChars++;
                }
            }

            // Stocare paragraf procesat
            comedyParagraphs++;
            comedy_text = (char *)realloc(comedy_text, comedyParagraphs * sizeToSend);
            strcat(comedy_text, paragraph);
            free(paragraph);

            MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, COMEDY_CONTROL_TAG, MPI_COMM_WORLD, &status);
            if (runningWorkersControl == 0) {
                break;
            }

        }

        // Trimitere cate un paragraf pe rand catre MASTER pentru afisare
        if (comedyParagraphs != 0) {
            int k = 0;
            for (int i = 0; i < comedyParagraphs; i++) {
                char paragraphToSend[sizeToSend];
                int j = 0;
                while(1) {
                    paragraphToSend[j] = comedy_text[k];
                    j++;
                    k++;
                    if ( (comedy_text[k] == '\n' && comedy_text[k - 1] == '\n') || comedy_text[k] == '\0') {
                        paragraphToSend[j] = '\n';
                        j++;
                        k++;
                        break;
                    }
                }
                paragraphToSend[j] = '\0';
                MPI_Send(paragraphToSend, sizeToSend, MPI_CHAR, MASTER, COMEDY_TEXT_TAG, MPI_COMM_WORLD);
            }
        }

    }


// WORKER FANTASY
    if (rank == FANTASY) {
        char *paragraph;
        char *fantasy_text;
        int fantasyParagraphs;

        fantasyParagraphs = 0;
        fantasy_text = (char *)malloc(sizeToSend);

        // Receive pentru controlul worker-ului
        MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, FANTASY_CONTROL_TAG, MPI_COMM_WORLD, &status);

        // PROCESARE TEXT
        while(runningWorkersControl) {
            paragraph = (char *)malloc(sizeToSend);
            strcpy(paragraph, "\0");

            // Receive la paragraph-ul trimis de thread-ul fantasy din master
            MPI_Recv(paragraph, sizeToSend, MPI_CHAR, MASTER, FANTASY_TEXT_TAG, MPI_COMM_WORLD, &status);
            
            // Procesare text fantasy
            if (paragraph[0] >= 97 && paragraph[0] <= 122) {
                paragraph[0] = paragraph[0] - 32;
            }
            for (int i = 0; i < strlen(paragraph); i++) {
                if ((paragraph[i] == ' ' || paragraph[i] == '\n') && paragraph[i + 1] >= 97 && paragraph[i + 1] <= 122) {
                    paragraph[i + 1] = paragraph[i + 1] - 32;
                }
            }    

            // Stocare paragraf procesat
            fantasyParagraphs++;
            fantasy_text = (char *)realloc(fantasy_text, fantasyParagraphs * sizeToSend);
            strcat(fantasy_text, paragraph);
            free(paragraph);

            // Receive pentru controlul worker-ului
            MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, FANTASY_CONTROL_TAG, MPI_COMM_WORLD, &status);
            if (runningWorkersControl == 0) {
                break;
            }

        }

        // Trimitere cate un paragraf pe rand catre MASTER pentru afisare
        if (fantasyParagraphs != 0) {
            int k = 0;
            for (int i = 0; i < fantasyParagraphs; i++) {
                char paragraphToSend[sizeToSend];
                int j = 0;
                while(1) {
                    paragraphToSend[j] = fantasy_text[k];
                    j++;
                    k++;
                    if ( (fantasy_text[k] == '\n' && fantasy_text[k - 1] == '\n') || fantasy_text[k] == '\0') {
                        paragraphToSend[j] = '\n';
                        j++;
                        k++;
                        break;
                    }
                }
                paragraphToSend[j] = '\0';
                MPI_Send(paragraphToSend, sizeToSend, MPI_CHAR, MASTER, FANTASY_TEXT_TAG, MPI_COMM_WORLD);
            }
        }        
    } 


// WORKER SCI-FI
    if (rank == SCIFI) {
        char *paragraph;
        char *scifi_text;
        int scifiParagraphs;

        scifiParagraphs = 0;
        scifi_text = (char *)malloc(sizeToSend);

        // Receive pentru controlul worker-ului
        MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, SCIFI_CONTROL_TAG, MPI_COMM_WORLD, &status);

        // PROCESARE TEXT
        while(runningWorkersControl) {
            paragraph = (char *)malloc(sizeToSend);
            strcpy(paragraph, "\0");

            // Receive la paragraph-ul trimis de thread-ul sci-fi din master
            MPI_Recv(paragraph, sizeToSend, MPI_CHAR, MASTER, SCIFI_TEXT_TAG, MPI_COMM_WORLD, &status);

            // Procesare text horror
            int toReverse = 1; // contor de reverse
            int lenToReverse = 0; // lungime cuvant de inversat
            char stringToReverse[50], stringReversed[50];
            strcpy(stringToReverse, "");
            strcpy(stringReversed, "");
            for (int i = 0; i < strlen(paragraph); i++) {
                if (paragraph[i] == ' ') {
                    toReverse++;
                }

                if (paragraph[i] == '\n') {
                    if (toReverse == 7) {
                        toReverse++;
                    } else {
                        toReverse = 1;
                    }
                }

                if (toReverse == 7) {
                    stringToReverse[lenToReverse] = paragraph[i];
                    lenToReverse += 1;
                }

                if (toReverse == 8) {
                    for (int y = 0; y < lenToReverse - 1; y++) {
                        stringReversed[y] = stringToReverse[lenToReverse - 1 - y];
                    }
                    int k = 0;
                    for (int j = i - lenToReverse + 1; j < i; j++) {
                        paragraph[j] = stringReversed[k];
                        k++;
                    }
                    toReverse = 1;
                    
                    for (int y = 0; y < lenToReverse; y++) {
                        stringReversed[y] = '\0';
                        stringToReverse[y] = '\0';
                    }
                    lenToReverse = 0;
                }
            }

            // Stocare paragraf procesat
            scifiParagraphs++;
            scifi_text = (char *)realloc(scifi_text, sizeToSend * scifiParagraphs);
            strcat(scifi_text, paragraph);
            free(paragraph);

            MPI_Recv(&runningWorkersControl, 1, MPI_INT, MASTER, SCIFI_CONTROL_TAG, MPI_COMM_WORLD, &status);
            if (runningWorkersControl == 0) {
                break;
            }
        }

        // Trimitere cate un paragraf pe rand catre MASTER pentru afisare
        if (scifiParagraphs != 0) {
            int k = 0;
            for (int i = 0; i < scifiParagraphs; i++) {
                char paragraphToSend[sizeToSend];
                int j = 0;
                while(1) {
                    paragraphToSend[j] = scifi_text[k];
                    j++;
                    k++;
                    if ( (scifi_text[k] == '\n' && scifi_text[k - 1] == '\n') || scifi_text[k] == '\0') {
                        paragraphToSend[j] = '\n';
                        j++;
                        k++;
                        break;
                    }
                }
                paragraphToSend[j] = '\0';
                MPI_Send(paragraphToSend, sizeToSend, MPI_CHAR, MASTER, SCIFI_TEXT_TAG, MPI_COMM_WORLD);
            }
        }
    }

    MPI_Finalize();

    return 0;
}
