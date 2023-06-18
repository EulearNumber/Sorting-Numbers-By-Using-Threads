#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>


typedef struct node
{

    struct node *r_node;
    struct node *l_node;
    unsigned value;

} Node;

typedef struct hash_table
{

    Node **list;
    unsigned nof_element;

} Hash_table;

typedef struct parameterPass
{

    unsigned *numbers;
    unsigned offset;
    unsigned last_ele;
    Hash_table *hash_table;
    pthread_mutex_t *mutex;

} parameterPass;

unsigned nof_Thread;
unsigned countNumOfElements(char *filename);
unsigned *readNumbers(char *filename, unsigned num_element);
Hash_table *initializeHashTable(unsigned numOfThread, unsigned numOfElements);
Node *createNode(unsigned value);
void *insertFunction(void *parameters);
void insertNode(Hash_table *ht, unsigned index, unsigned value);
void print_hash_table(Hash_table *hash_table);
void bubbleSort(Node *start);
void *sortingFunction(void *parameters);
void swap(Node *a, Node *b);
void freeHashTable(Hash_table *ht);
int main(int argc, char* argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <CSV filename> <numOfThreads>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    int numOfElements;
    Node *node = NULL;
    Hash_table *hash_table = NULL;
    unsigned *Numbers;
    char* filename = argv[1];
    nof_Thread = atoi(argv[2]);
    //printf("Enter the number of thread:\n");
    //scanf("%u", &nof_Thread);
    numOfElements = countNumOfElements(filename);
    Numbers = readNumbers(filename, numOfElements);
    hash_table = initializeHashTable(nof_Thread, numOfElements);
    pthread_t threads[nof_Thread];
    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);
    parameterPass pp[nof_Thread];
    printf("Threads creation started \n");
    for (unsigned i = 0; i < nof_Thread; i++)
    {
        unsigned interval = numOfElements / nof_Thread + 1;
        unsigned offset = interval * i;
        unsigned last_ele = offset + interval;
        if (last_ele > numOfElements)
        {
            last_ele = numOfElements;
        }

        pp[i].numbers = Numbers;
        pp[i].offset = offset;
        pp[i].last_ele = last_ele;
        pp[i].hash_table = hash_table;
        pp[i].mutex = &mutex;

        pthread_create(&threads[i], NULL, insertFunction, &pp[i]);
    }

    for (unsigned i = 0; i < nof_Thread; i++)
    {
        pp[i].offset = i;
        pthread_join(threads[i], NULL);
    }

    printf("Printing is done..\n");

    // print_hash_table(hash_table);

    printf("Sorting threads are being created..\n");
    unsigned numOfSortingThreads = (nof_Thread * (nof_Thread + 1)) / 2; 
    pthread_t sortingThreads[numOfSortingThreads];
    parameterPass sortingParams[numOfSortingThreads];
    for (unsigned i = 0; i < numOfSortingThreads; i++)
    {
        sortingParams[i].hash_table = hash_table;
        sortingParams[i].offset = i;
        sortingParams[i].numbers = NULL;
        sortingParams[i].mutex = &mutex;

        pthread_create(&sortingThreads[i], NULL, sortingFunction, &sortingParams[i]);
    }
    printf("Creation is done..\n");

    for (unsigned i = 0; i < numOfSortingThreads; i++)
    {
        pthread_join(sortingThreads[i], NULL);
    }

    printf("Joining is completed..\n");

    printf("Printing is done..\n");
    print_hash_table(hash_table);

    pthread_mutex_destroy(&mutex);

    freeHashTable(hash_table);
    free(Numbers);
    return 0;
}

unsigned countNumOfElements(char *filename)
{
    FILE *fp;
    unsigned int num_of_elements = 0;
    char ch;

    fp = fopen(filename, "r");

    for (ch = getc(fp); ch != EOF; ch = getc(fp))
        if (ch == '\n')
            num_of_elements = num_of_elements + 1;

    rewind(fp);
    fclose(fp);

    return num_of_elements;
}

unsigned *readNumbers(char *filename, unsigned num_element)
{
    FILE *fp;
    unsigned *Numbers = (unsigned *)malloc(sizeof(unsigned) * num_element);
    char line[100];
    int index = 0;
    fp = fopen(filename, "r");

    while (fgets(line, sizeof(line), fp) != NULL)
    {
        if (strstr(line, "number_id,number") != NULL)
        {
            continue;
        }
        int number_id, number;
        sscanf(line, "%d,%d", &number_id, &number);

        if (index < num_element)
        {
            Numbers[index] = number;
            index++;
        }
    }
    fclose(fp);

    return Numbers;
}

Hash_table *initializeHashTable(unsigned numOfThread, unsigned numOfElements)
{
    unsigned size = (numOfThread * (numOfThread + 1)) / 2;
    Hash_table *hash_table = (Hash_table *)malloc(sizeof(Hash_table));
    hash_table->list = (Node **)malloc(sizeof(Node *) * size);
    hash_table->nof_element = numOfElements;

    for (int i = 0; i < size; i++)
    {
        hash_table->list[i] = NULL;
    }

    return hash_table;
}

Node *createNode(unsigned value)
{
    Node *newNode = (Node *)malloc(sizeof(Node));
    if (!newNode)
    {
        perror("Error allocating memory");
        exit(EXIT_FAILURE);
    }

    newNode->value = value;
    newNode->r_node = NULL;
    newNode->l_node = NULL;

    return newNode;
}
unsigned take_modula(unsigned figure)
{

    unsigned modula;
    modula = figure % ((nof_Thread * (nof_Thread + 1)) / 2);
    return modula;
}
void insertNode(Hash_table *ht, unsigned index, unsigned value)
{

    Node *new_node = createNode(value);
    new_node->r_node = ht->list[index];
    ht->list[index] = new_node;
}

void *insertFunction(void *parameters)
{
    parameterPass *pp = (parameterPass *)parameters;
    unsigned index;
    for (unsigned i = pp->offset; i < pp->last_ele; i++)
    {
        unsigned value = pp->numbers[i];
        unsigned index = take_modula(value);

        pthread_mutex_lock(pp->mutex);
        insertNode(pp->hash_table, index, value);
        pthread_mutex_unlock(pp->mutex);
    }
    return NULL;
}
void *sortingFunction(void *parameters)
{
    parameterPass *pp = (parameterPass *)parameters;
    bubbleSort(pp->hash_table->list[pp->offset]);
    
    return NULL;
}

void swap(Node *a, Node *b)
{
    unsigned temp = a->value;
    a->value = b->value;
    b->value = temp;
}

void bubbleSort(Node *start)
{
    if (start == NULL || (start)->r_node == NULL)
    {
        printf("Start is NULL \n");
        return;
    }
    int swapped;
    Node *current;
    Node *last = NULL;

    do
    {
        swapped = 0;
        current = start;

        while (current->r_node != last)
        {
            if (current->value > current->r_node->value)
            {
                swap(current, current->r_node);
                swapped = 1;
            }
            current = current->r_node;
        }

        last = current;
    } while (swapped);
}

void print_hash_table(Hash_table* hash_table) {
    unsigned size = nof_Thread * ((nof_Thread + 1) / 2);
    unsigned index;
    char input;
    
    while (1) {
        
        printf("Enter the index that you want to see.\n");
        printf("Available mods are:\n");
        
        for (unsigned i = 0; i < size; i++) {
            printf("%u ", i);
        }
        
        

        printf("\nEnter the index: ");
        scanf("%u", &index);
        
        
        Node* temp = hash_table->list[index];
        
        while (temp != NULL) {
            printf("%u\n", temp->value);
            temp = temp->r_node;
        }
        printf("Enter 'e' to exit or enter any other key to continue to seeing specific index: \n");
        scanf("%c", &input);

        getchar();
        if (input == 'e') { // Break the loop if the input is 'e'
            printf("'e' button pressed. Exiting...\n");
            break;
        }

    }
}


void freeHashTable(Hash_table *ht)
{
    for (unsigned i = 0; i < (nof_Thread * (nof_Thread + 1)) / 2; i++)
    {
        Node *current = ht->list[i];
        while (current)
        {
            Node *temp = current;
            current = current->r_node;
            free(temp);
        }
    }
    free(ht->list);
    free(ht);
}
