# typing 'make' will invoke the first target entry in the file 
# (in this case the default target entry)

default: analisi

analisi: analisi.c
	gcc -O analisi.c -o analisi -lpthread

debug: analisi.c
	gcc -g analisi.c -o debug -lpthread

# To start over from scratch, type 'make clean'.  This
# removes the executable file, as well as old .o object
# files and *~ backup files:
#
clean: 
	rm analisi debug
