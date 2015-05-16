#include "./M_queue.h"

// инициализация очереди
M_queue::M_queue()
{
    int rc;
    buff[0] = '1';
    // инициализация мьютекса
    pthread_mutex_init(&lock, NULL);
    // открытие пайпа
    rc = pipe(sig_pipe);
    assert_err(rc == 0);
    fl = 0;
}

// удаление очереди
M_queue::~M_queue()
{
    pthread_mutex_lock(&lock);
    // закрытие пайпа
    close(sig_pipe[0]);
    close(sig_pipe[1]);
    pthread_mutex_unlock(&lock);

    // удаление мьютекса
    pthread_mutex_destroy(&lock);
}

// запись в очередь
void M_queue::push(void *idt, int *error)
{
    // запись в очередь
    pthread_mutex_lock(&lock);
    // запись данных
    q.push(idt);
    // signal
    if (fl == 1)
    {
        // очередь пустая
        fl = 0;
        *error = write(sig_pipe[1], buff, 1);
    }
    else
    {
        *error = 1;
    }
    pthread_mutex_unlock(&lock);
}

// выборка из очереди
void *M_queue::pop(int *error)
{
    void *idt;

    pthread_mutex_lock(&lock);
    // проверяем есть ли еще данные
    if (q.empty() == 1)
    {
        fl = 1;
        pthread_mutex_unlock(&lock);
        // ждем данные
        *error = read(sig_pipe[0], buff, 1);
        if (*error == 0)
        {
            return NULL;
        }
        pthread_mutex_lock(&lock);
    }
    else
    {
        *error = 1;
    }
    // считывание входных данных
    idt = q.front();
    q.pop();
    pthread_mutex_unlock(&lock);

    return idt;
}