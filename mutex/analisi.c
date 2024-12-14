#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>

#define MAXCHAR 500

#define LEN_CODE_AIRPORT 3
#define STR_CODE_AIRPORT (LEN_CODE_AIRPORT+1) // Incluimos el caracter de final de palabra '\0'
#define NUM_AIRPORTS 303

#define COL_ORIGIN_AIRPORT 17
#define COL_DESTINATION_AIRPORT 18

typedef struct cell {
  int nelems;
  char **lines;
} cell;

typedef struct P {
  int B, N, **num_flights, *nblocks, *j;
  char **airports;
  cell **buffer;
  pthread_mutex_t *mutex_mat, *mutex_buffer;
  pthread_cond_t *cua_cons, *cua_prod;
} parameters;

/**
 * Reserva espacio para una matriz de tamaño nrow x ncol,
 * donde cada elemento de la matriz tiene size bytes, se 
 * inicializa a 0 si asi se especifica
 */

void **malloc_matrix(int nrow, int ncol, size_t size, bool init)
{
  int i;
  void **ptr;

  ptr = malloc(sizeof(void *) * nrow);
  for(i = 0; i < nrow; i++)
    ptr[i] = init ? calloc(ncol, size) : malloc(size * ncol);

  return ptr;
}

/**
 * Libera una matriz de tamaño con nrow filas. Utilizar
 * la funcion malloc_matrix para reservar la memoria 
 * asociada.
 */

void free_matrix(void **matrix, int nrow)
{
  int i;

  for(i = 0; i < nrow; i++)
    free(matrix[i]);

  free(matrix);
}

// Reservamos memoria para una celda (con espacio para un bloque de N lineas)
cell *allocate_cell(int N) {
    cell *c = (cell *) malloc(sizeof(cell));
    c->lines = (char **) malloc_matrix(N, MAXCHAR, sizeof(char), false);
    return c;
}

// Liberamos la memoria reservada para una celda (con espacio para un bloque de N lineas)
void free_cell(int N, cell *c) {
    free_matrix((void **) c->lines, N);
    free(c);
}

/**
 * Leer el fichero fname que contiene los codigos
 * IATA (3 caracteres) de los aeropuertos a analizar.
 * En total hay NUM_AIRPORTS a leer, un valor prefijado
 * (para simplificar el código). Los codigos de los
 * aeropuertos se alacenan en la matriz airports, una
 * matriz cuya memoria ha sido previamente reservada.
 */

void read_airports(char **airports, char *fname) 
{
  int i;
  char line[MAXCHAR];

  FILE *fp;

  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';

  fp = fopen(fname, "r");
  if (!fp) {
    printf("ERROR: could not open file '%s'\n", fname);
    exit(1);
  }

  i = 0;
  while (i < NUM_AIRPORTS)
  {
    fgets(line, 100, fp);
    line[3] = eow; 

    /* Copiamos los datos al vector */
    strcpy(airports[i], line);

    i++;
  }

  fclose(fp);
}

/**
 * Dada la matriz de con los codigos de los aeropuertos,
 * así como un código de aeropuerto, esta función retorna
 * la fila asociada al aeropuerto.
 */

int get_index_airport(char *code, char **airports)
{
  int i;

  for(i = 0; i < NUM_AIRPORTS; i++) 
    if (strcmp(code, airports[i]) == 0)
      return i;

  return -1;
}


/**
 * Dada la matriz num_flights, se imprimen por pantalla el
 * numero de destinos diferentes que tiene cada aeropuerto.
 */

void print_num_flights_summary(int **num_flights, char **airports)
{
  int i, j, num;

  for(i = 0; i < NUM_AIRPORTS; i++) 
  {
    num = 0;

    for(j = 0; j < NUM_AIRPORTS; j++)
    {
      if (num_flights[i][j] > 0)
        num++;
    }

    printf("Origin: %s -- Number of different destinations: %d\n", airports[i], num);
  }
}

/**
 * Esta funcion se utiliza para extraer informacion del fichero CSV que
 * contiene informacion sobre los vuelos. En particular, dada una linea
 * leida de fichero, la funcion extrae el origen y destino de los vuelos.
 */

int extract_fields_airport(char *origin, char *destination, char *line) 
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
   * contenedor del substring a copiar
   */
  char word[STR_CODE_AIRPORT];
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
      if (coma_count ==  COL_ORIGIN_AIRPORT || coma_count == COL_DESTINATION_AIRPORT) {
        /*
         * Calculamos la longitud, si es mayor que 1 es que tenemos 
         * algo que copiar
         */
        len = end - start;

        if (len > 1) {

          if (len > STR_CODE_AIRPORT) {
            printf("ERROR len code airport\n");
            exit(1);
          }

          /*
           * Copiamos el substring
           */
          for(iterator = start; iterator < end-1; iterator ++){
            word[iterator-start] = line[iterator];
          }
          /*
           * Introducimos el caracter de fin de palabra
           */
          word[iterator-start] = eow;
          /*
           * Comprobamos que el campo no sea NA (Not Available) 
           */
          if (strcmp("NA", word) == 0)
            invalid = 1;
          else {
            switch (coma_count) {
              case COL_ORIGIN_AIRPORT:
                strcpy(origin, word);
                found++;
                break;
              case COL_DESTINATION_AIRPORT:
                strcpy(destination, word);
                found++;
                break;
              default:
                printf("ERROR in coma_count\n");
                exit(1);
            }
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

// Modificamos los punteros para cambiar las celdas
void swap_cells(cell **c1, cell **c2) {
  cell *tmp = *c2;
  *c2 = *c1;
  *c1 = tmp;
}

// Procesamiento concurrente de los datos
void *process_data(void *arg) 
{
  char origin[STR_CODE_AIRPORT], destination[STR_CODE_AIRPORT];
  int invalid, index_origin, index_destination, i, *j, *nblocks;
  bool process = true;
  cell *c;
  parameters *par = (parameters *) arg;

  j = par->j;
  nblocks = par->nblocks;

  // Reservamos espacio para la celda del consumidor
  c = allocate_cell(par->N);

  // Procesamos el fichero hasta que no se haya llegado al final
  while (process) {
    // Lectura del buffer: mientras este vacio esperamos (SECCION CRITICA, no if porque otro consumidor podria adelantarnos desde fuera y podriamos estar sin bloques a leer al despertarnos)
    pthread_mutex_lock(par->mutex_buffer);
    while (*nblocks == 0)
      pthread_cond_wait(par->cua_cons, par->mutex_buffer);

    // Transferimos el bloque del buffer para procesarlo
    swap_cells(&c, &par->buffer[*j]);
    *nblocks = *nblocks - 1;

    // Apuntamos a la siguiente celda del buffer (debemos modificar este indice dentro de la 
    // seccion critica para que otro consumidor sepa cual es la siguiente celda a procesar)
    *j = (*j + 1) % par->B;

    // Fin de la seccion critica, despertamos al productor si esta esperando que haya alguna celda libre
    pthread_cond_signal(par->cua_prod);
    pthread_mutex_unlock(par->mutex_buffer);

    // Si hay lineas a leer, las procesamos. Si no, hemos terminado
    if (c->nelems == 0) {
      process = false;
    } else {
      // Procesamos las nelems lineas leidas (seran N si no es el ultimo bloque del fichero)
      for (i = 0; i < c->nelems; i++) {
        invalid = extract_fields_airport(origin, destination, c->lines[i]);
        if (!invalid) {
          // No es necesario proteger el acceso a airports ya que una vez ejecutada
          // la funcion read_airports su contenido no se modifica en todo el programa
          index_origin = get_index_airport(origin, par->airports);
          index_destination = get_index_airport(destination, par->airports);

          if ((index_origin >= 0) && (index_destination >= 0)) {
            // Protegemos la actualizacion de la matriz num_flights (SECCION CRITICA)
            pthread_mutex_lock(par->mutex_mat); 
            par->num_flights[index_origin][index_destination]++;
            pthread_mutex_unlock(par->mutex_mat);
          }
        }
      }
    }
  }

  // Liberamos la memoria reservada para la celda del consumidor
  free_cell(par->N, c);

  return ((void *) 0);
}

// Transferimos la celda del productor al buffer
void transfer_prod_cell(int *k, cell **c, parameters *par) {
    // Escritura en el buffer: si esta lleno esperamos (buffer: SECCION CRITICA, protegemos, con if basta ya que solo hay uno en cua_prod)
    pthread_mutex_lock(par->mutex_buffer);
    if (*(par->nblocks) == par->B)
      pthread_cond_wait(par->cua_prod, par->mutex_buffer);

    // Transferimos el bloque leido al buffer
    swap_cells(c, &par->buffer[*k]);
    *(par->nblocks) = *(par->nblocks) + 1;

    // Fin de la seccion critica, despertamos al primer consumidor que espere datos a procesar
    pthread_cond_signal(par->cua_cons); 
    pthread_mutex_unlock(par->mutex_buffer);

    // Apuntamos a la siguiente celda del buffer
    *k = (*k + 1) % par->B;
}

/**
 * Dado un fichero CSV que contiene informacion entre multiples aeropuertos,
 * esta funcion lee cada linea del fichero y actualiza la matriz num_flights
 * para saber cuantos vuelos hay entre cada ciudad origen y destino.
 */ 

void read_airports_data(int **num_flights, char **airports, char *fname, int F, int B, int N) 
{
  int i, j, k, nblocks;
  pthread_t *ntid;
  pthread_mutex_t mutex_mat, mutex_buffer;
  pthread_cond_t cua_cons, cua_prod;
  cell **buffer, *c;
  FILE *fp;

  // Reservamos espacio para el buffer
  buffer = (cell **) malloc(sizeof(cell *) * B);
  for(i = 0; i < B; i++) {
    buffer[i] = allocate_cell(N);
  }
  nblocks = 0;
  j = 0;

  // Creamos los hilos secundarios (iniciando los mutex para proteger las secciones criticas)
  pthread_mutex_init(&mutex_mat, NULL);
  pthread_mutex_init(&mutex_buffer, NULL);
  pthread_cond_init(&cua_cons, NULL);
  pthread_cond_init(&cua_prod, NULL);
  parameters par = {B, N, num_flights, &nblocks, &j, airports, buffer, &mutex_mat, &mutex_buffer, &cua_cons, &cua_prod};
  ntid = (pthread_t *) malloc(F * sizeof(pthread_t));
  for (i = 0; i < F; i++) {
    if (pthread_create(ntid+i, NULL, process_data, (void *) &par) != 0) {
      printf("ERROR: could not create thread number %d\n", i);
      exit(1);
    }
  }

  // Reservamos espacio para la celda del productor
  c = allocate_cell(N);

  // Abrimos el fichero a procesar
  fp = fopen(fname, "r");
  if (!fp) {
    printf("ERROR: could not open '%s'\n", fname);
    exit(1);
  }

  /* Leemos la cabecera del fichero */
  fgets(c->lines[0], MAXCHAR, fp);

  // Leemos del fichero hasta que no se haya llegado al final
  k = 0;
  while (!feof(fp)) {
    // Leemos N lineas seguidas del fichero (si quedan suficientes)
    i = 0;
    while (i < N && fgets(c->lines[i], MAXCHAR, fp) != NULL) {
      i++;
    }
    c->nelems = i;

    // Transferimos el bloque leido al buffer
    transfer_prod_cell(&k, &c, &par);
  }

  // Cerramos el fichero
  fclose(fp);

  // Enviamos los F bloques vacios a los consumidores para que finalicen
  for (i = 0; i < F; i++) {
    c->nelems = 0;
    transfer_prod_cell(&k, &c, &par);
  }

  // Esperamos a que finalicen los hilos secundarios
  for (i = 0; i < F; i++) {
    pthread_join(ntid[i], NULL);
  }

  // Liberamos la memoria dinamica reservada
  free(ntid);
  for (i = 0; i < B; i++) {
    free_cell(N, buffer[i]);
  }
  free(buffer);
  free_cell(N, c);
}

/**
 * Esta es la funcion principal que realiza los siguientes procedimientos
 * a) Lee los codigos IATA del fichero de aeropuertos
 * b) Lee la informacion de los vuelos entre diferentes aeropuertos y
 *    actualiza la matriz num_flights correspondiente.
 * c) Se imprime para cada aeropuerto origen cuantos destinos diferentes
 *    hay.
 * d) Se imprime por pantalla lo que ha tardado el programa para realizar
 *    todas estas tareas.
 */

int main(int argc, char **argv)
{
  char **airports;
  int **num_flights;
  int F, B, N;

  if (argc != 6) {
    printf("%s <aeroports.csv> <flights.csv> <F> <B> <N>\n", argv[0]);
    exit(1);
  }
  
  F = atoi(argv[3]);
  B = atoi(argv[4]);
  N = atoi(argv[5]);
  if (F <= 0 || B <= 0 || N <= 0) {
    printf("F, B and N must be positive integers!\n");
    exit(1);
  }

  struct timeval tv1, tv2;

  // Tiempo cronologico 
  gettimeofday(&tv1, NULL);

  // Reserva espacio para las matrices
  airports    = (char **) malloc_matrix(NUM_AIRPORTS, STR_CODE_AIRPORT, sizeof(char), false);
  num_flights = (int **) malloc_matrix(NUM_AIRPORTS, NUM_AIRPORTS, sizeof(int), true);

  // Lee los codigos de los aeropuertos 
  read_airports(airports, argv[1]);

  // Lee los datos de los vuelos
  read_airports_data(num_flights, airports, argv[2], F, B, N);

  // Imprime un resumen de la tabla
  print_num_flights_summary(num_flights, airports);

  // Libera espacio
  free_matrix((void **) airports, NUM_AIRPORTS);
  free_matrix((void **) num_flights, NUM_AIRPORTS);

  // Tiempo cronologico 
  gettimeofday(&tv2, NULL);

  // Tiempo para la creacion del arbol 
  printf("Tiempo para procesar el fichero: %f segundos\n",
      (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
      (double) (tv2.tv_sec - tv1.tv_sec));

  return 0;
}
