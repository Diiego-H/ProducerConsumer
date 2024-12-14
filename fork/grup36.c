#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#define filename1 "col8.bin"
#define filename2 "col9.bin"

// Flags to know if a signal has been received
int flag1 = 0, flag2 = 0;

int get_column_fields_airport(int *column8, int *column9, char *line);
void sigusr1(int sign);
void sigusr2(int sign);
void waitsigusr1();
void waitsigusr2();
void notifyConsumers(int c1, int c2);
void consumer1(int p, int N);
void consumer2(int p, int N);
void producer(FILE *file, int c1, int c2, int N);

void sigusr1(int sign) {
    flag1 = 1;
}

void sigusr2(int sign) {
    flag2 = 1;
}

int main(int argc, char *argv[])
{
    FILE *file;
    int c1, c2, p;

    if(argc != 3)
    {
        printf("ERROR: %s <file> <N>\n", argv[0]);
        exit(1);
    }

    int N = atoi(argv[2]);
    if (N <= 0) {
	      printf("ERROR: N must be a positive number\n");
	      exit(1);
    }

    char *filename = argv[1];
    file = fopen(filename, "r");
    if (!file) {
        printf("ERROR: Could not open '%s'\n", filename);
        exit(1);
    }

    // Signal communication
    signal(SIGUSR1, sigusr1);
    signal(SIGUSR2, sigusr2);

    // We create the consumers
    p = getpid();
    c1 = fork();
    if (c1 == 0) {
        consumer1(p, N);
        return 0;
    }
    c2 = fork();
    if (c2 == 0) {
        consumer2(p, N);
        return 0;
    }

    producer(file, c1, c2, N);
    return 0;
}

void waitsigusr1() {
    while(!flag1);
    flag1 = 0;
}

void waitsigusr2() {
    while(!flag2);
    flag2 = 0;
}

void consumer1(int p, int N) {
    int n, fd, vector[N], num_elements = 0;
    unsigned long int passenger_count = 0;

    waitsigusr1();
    kill(p, SIGUSR1);
    fd = open(filename1, O_RDONLY);
    do {
        waitsigusr1();
        kill(p, SIGUSR1);
        n = read(fd, vector, N * sizeof(int)) / sizeof(int);
        // Process the data received
        for (int i = 0; i < n; i++) {
          passenger_count += vector[i];
        }
        num_elements += n;
    } while (n == N);
    close(fd);

    // Print the mean, notify producer and finish
    waitsigusr1();
    printf("Consumer 1: Number of elements: %lu\n", num_elements);
    printf("Consumer 1: Mean of passengers: %f\n", ((float) passenger_count) / num_elements);
    kill(p, SIGUSR1);
}

void consumer2(int p, int N) {
    int n, fd, vector[N], num_elements = 0;
    unsigned long int trip_time_in_secs = 0;

    waitsigusr2();
    kill(p, SIGUSR2);
    fd = open(filename2, O_RDONLY);
    do {
        waitsigusr2();
        kill(p, SIGUSR2);
        n = read(fd, vector, N * sizeof(int)) / sizeof(int);
        // Process the data received
        for (int i = 0; i < n; i++) {
          trip_time_in_secs += vector[i];
        }
        num_elements += n;
    } while (n == N);
    close(fd);

    // Print the mean, notify producer and finish
    waitsigusr2();
    printf("Consumer 2: Number of elements: %lu\n", num_elements);
    printf("Consumer 2: Mean of trip time: %f\n", ((float) trip_time_in_secs) / num_elements);
    kill(p, SIGUSR2);
}

// Notify consumers and wait until they inform the notification has been received
void notifyConsumers(int c1, int c2) {
    kill(c1, SIGUSR1);
    kill(c2, SIGUSR2);
    waitsigusr1();
    waitsigusr2();
}

void producer(FILE *file, int c1, int c2, int N) {
    char line[256];
    int invalid, value_col8, value_col9, n, fd1, fd2;

    // Vectors in which data will be kept before writing it in the files
    int vector1[N], vector2[N];

    // The producer opens the files and notifies the consumers
    fd1 = open(filename1, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    fd2 = open(filename2, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    notifyConsumers(c1, c2);

    // We ignore the header of the file.
    fgets(line, sizeof(line), file);

    // Read the whole file
    n = 0;
    while (fgets(line, sizeof(line), file))
    {
        invalid = get_column_fields_airport(&value_col8, &value_col9, line);
      
        if (!invalid) {
            vector1[n] = value_col8; 
            vector2[n] = value_col9;

            // Write the N elements and notify consumers
            if (++n == N) {
                n = 0;
                write(fd1, vector1, N * sizeof(int));
                write(fd2, vector2, N * sizeof(int));
                notifyConsumers(c1, c2);
            }
        }
    }

    fclose(file);

    // Write the remaining data, close files and notify consumers
    write(fd1, vector1, n * sizeof(int));
    write(fd2, vector2, n * sizeof(int));
    close(fd1);
    close(fd2);
    notifyConsumers(c1, c2);

    //Wait until consumers finish
    notifyConsumers(c1, c2);
    
    //Remove files and finish
    remove(filename1);
    remove(filename2);
}

/**
 * Esta funcion se utiliza para extraer informacion del fichero CSV que
 * contiene informacion sobre los trayectos. En particular, dada una linea
 * leida de fichero, la funcion extrae las columnas 8 y 9. 
 */

#define STRLEN_COLUMN 10
#define COLUMN8 8
#define COLUMN9 9

int get_column_fields_airport(int *column8, int *column9, char *line) 
{
  /*Recorre la linea por caracteres*/
  char caracter;
  /* i sirve para recorrer la linea
   * iterator es para copiar el substring de la linea a char
   * coma_count es el contador de comas
   */
  int i, iterator, coma_count;
  /* start indica donde empieza el substring a copiar
   * end indica donde termina el substring a copiar
   * len indica la longitud del substring
   */
  int start, end, len;
  /* invalid nos permite saber si todos los campos son correctos
   * 1 hay error, 0 no hay error 
   */
  int invalid = 0;
  /* found se utiliza para saber si hemos encontrado los dos campos:
   * origen y destino
   */
  int found = 0;
  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';
  /*
   * substring para extraer la columna
   */
  char substring[STRLEN_COLUMN];
  /*
   * Inicializamos los valores de las variables
   */
  start = 0;
  end = -1;
  i = 0;
  coma_count = 0;
  /*
   * Empezamos a contar comas
   */
  do {
    caracter = line[i++];
    if (caracter == ',') {
      coma_count ++;
      /*
       * Cogemos el valor de end
       */
      end = i;
      /*
       * Si es uno de los campos que queremos procedemos a copiar el substring
       */
      if (coma_count == COLUMN8 || coma_count == COLUMN9) {
        /*
         * Calculamos la longitud, si es mayor que 1 es que tenemos 
         * algo que copiar
         */
        len = end - start;

        if (len > 1) {

          if (len > STRLEN_COLUMN) {
            printf("ERROR STRLEN_COLUMN\n");
            exit(1);
          }

          /*
           * Copiamos el substring
           */
          for(iterator = start; iterator < end-1; iterator ++){
            substring[iterator-start] = line[iterator];
          }
          /*
           * Introducimos el caracter de fin de palabra
           */
          substring[iterator-start] = eow;
          /*
           * Comprobamos que el campo no sea NA (Not Available) 
           */

          switch (coma_count) {
            case COLUMN8:
              *column8 = atoi(substring); 
              found++;
              break;
            case COLUMN9:
              *column9 = atoi(substring); 
              found++;
              break;
            default:
              printf("ERROR in coma_count\n");
              exit(1);
          }

        } else {
          /*
           * Si el campo esta vacio invalidamos la linea entera 
           */

          invalid = 1;
        }
      }
      start = end;
    }
  } while (caracter && invalid==0);

  if (found != 2)
    invalid = 1;

  return invalid;
}
