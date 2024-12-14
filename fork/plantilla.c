#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int get_column_fields_airport(int *column8, int *column9, char *line);

int main(int argc, char *argv[])
{
    FILE *file;
    char line[256];

    int invalid, value_col8, value_col9;
    unsigned long int num_elements, passenger_count, trip_time_in_secs;

    if(argc != 2)
    {
        printf("%s <file>\n", argv[0]);
        exit(1);
    }

    char *filename = argv[1];

    num_elements = 0;
    passenger_count = 0;
    trip_time_in_secs = 0;

    file = fopen(filename, "r");
    if (!file) {
        printf("ERROR: could not open '%s'\n", filename);
        exit(1);
    }

    // We ignore the header of the file.
    fgets(line, sizeof(line), file);

    // Read the whole file
    while (fgets(line, sizeof(line), file))
    {
      invalid = get_column_fields_airport(&value_col8, &value_col9, line);
      
      if (!invalid) {
        passenger_count += value_col8; 
        trip_time_in_secs += value_col9;
        num_elements++;
      }
    }

    fclose(file);
    
    if (num_elements == 0) {
        printf("Number of elements is zero!!!\n");
        exit(1);
    }

    float pc = 0, tt = 0;
    pc = (float)passenger_count/(float)num_elements;	
    tt = (float)trip_time_in_secs/(float)num_elements;

    printf("Main: Aplication read %ld elements\n", num_elements);
    printf("Main: Mean of passengers: %f\n", pc);
    printf("Main: Mean of trip time: %f\n secs", tt);

    return 0;
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
