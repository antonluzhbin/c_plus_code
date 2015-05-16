/* 
 * File:   show.h
 * Author: anton
 *
 * Created on 13 Июнь 2013 г., 12:07
 */

#ifndef SHOW_H
#define	SHOW_H

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include <stdint.h>
#include <sys/time.h>
#include "../../logger_cpp/Lgr.h"
#include "../../db_conn/db.h"
#include "strto.h"
#include "budget_log.h"
#include "base_show.h"

#define log_debug

class show_t: public base_show_t 
{
    
public:
    
    struct answer_budget_t
    {
        uint32_t unix_time;             // временная метка
        
        uint16_t num_log;               // номер файла лога
        
        uint8_t send_status;            // флаг ошибки: 1 - была ошибка при передаче
    };
    
    show_t(ucs_t *uc_, Lgr *log_);
    
    virtual ~show_t();
    
    // считывание кода заявок для показа
    int code_show(db_t *db);
   
    // отправка ответа в бюджет
    int send_answer_budget(budget_log_t *budget_log);
    
    // отправка показа в статистику
    int send_answer_statistics();
    
    // проверка есть ли сворачивания ТГ группы
    int swerving_tg();
                
    // запись ТГ своричиваний
    void save_swerving_tg(db_t *db);
  
private:
    
    // выходные данные бюджет
    answer_budget_t answer_budget;
    
    show_t();
    
    show_t(const show_t& orig);
       
    // сборка ответа для статистики
    int build_answer_statistics(char **p_buf, int *p_len);
    
    // сборка ответа для бюджета
    int build_answer_budget(char **p_buf, int *p_len, answer_budget_t *answer_budget, int index);
          
    // сборка даннях для записи в лог бюджета
    int build_log_budget(char **p_buf, int *p_len, answer_budget_t *answer_budget, int index);
  
    // вычисление размера ответа в зависимости от типа
    int size_answer(type_unit_t type);
    
    // сборка js кода (для nginx)
    int build_js_code();
    
};

#endif	/* SHOW_H */

