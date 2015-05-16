#ifndef M_QUEUE_H
#define	M_QUEUE_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <queue>
#include "../assert.h"

// класс защищенной очереди с пайпом
// в q записываются данные
// через sig_pipe передается сигнал о поступивших данных
class M_queue
{
    public:
        M_queue();
        ~M_queue();

        // запись в очередь
        void push(void *idt, int *error);
        // извлечение из очереди
        void *pop(int *error);

    private:
        std::queue<void *> q;     // очередь
        int sig_pipe[2];                // пайп для передачи сигнала о поступивщих данных
        pthread_mutex_t lock;           // внутренний мьютекс
        char buff[1];                   // буффер для пайпа
        int fl;                         // флаг
};


#endif	/* M_QUEUE_H */

