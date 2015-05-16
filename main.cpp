/* 
 * File:   main.cpp
 * Author: anton
 *
 * Created on 13 Июнь 2013 г., 8:30
 */

#include "storage.h"

// включение вывода логов
//#define log_crit
//#define log_level_crit_global 9
//#define log_warn
//#define log_level_warn_global 9 
#define log_debug
#define log_level_debug_global 1

pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;

int main(int argc, char** argv) 
{   
    int rc;
    
    // лог
    Lgr out_log;
    set_log_global(&out_log);
    
    // показ
    // очередь
    M_queue *input_show = new M_queue;
    
    // параметры для треда
    parameters_t parameters = set_parameters(input_show,  &out_log);
    
    // запуск треда
    pthread_t thr_show;
    pthread_create(&thr_show, NULL, thread_show, (void *)&parameters);
    
    // дефолтный показ
    // очередь
    M_queue *input_default_show = new M_queue;
    
    // параметры для треда
    parameters_t parameters_default = set_parameters(input_default_show, &out_log);        
    
    // запуск треда
    pthread_t thr_default_show;
    pthread_create(&thr_default_show, NULL, thread_default_show, (void *)&parameters_default);
    
    // клик
    // очередь
    M_queue *input_click = new M_queue;
    
    // параметры для треда
    parameters_t parameters_click = set_parameters(input_click, &out_log);        
    
    // запуск треда
    pthread_t thr_click;
    pthread_create(&thr_click, NULL, thread_click, (void *)&parameters_click);
    
    char buf[1024];
    char *p_buf;
    int i, len;
    uint8_t type;
    uint cnt[3] = { 0, 0, 0 };
    
    ucs_message_t buff[UCS_INSIDE_RCV_BUFF_SIZE];
    uint recv_count, recv_index;
    int size_request;
    
    ucs_t *uc;
    
    if (argc != 2)
    {
        out_log.LGR_MCRIT("STOR: Wrong input stringUsage: %s config_file", argv[0]);
        return 1;
    }          
            
    // включение модуля в систему
    uc = new ucs_t(argv[1], units, UNIT_COUNT);

    ucs_name_t un;
    // считываем название модуля
    uc->get_unit_name(un);
    out_log.LGR_MDEBG("unit name = \"%s\"", un);
    
    for ( ;; )
    {        
        recv_count = uc->recv(buff);
        out_log.LGR_MDEBG("recv count = %d", recv_count);
        for (recv_index = 0; recv_index < recv_count; recv_index++)
        {
            if (buff[recv_index].type == UCS_MESSAGE_TYPE_DATA)
            {
                switch (buff[recv_index].address)
                {
                    case MODULE_MAIN:
                    case MODULE_GEO:
                    case MODULE_CLICKER:
                        
                        size_request = buff[recv_index].get_size();
                        memcpy(buf, buff[recv_index].get_data(), size_request);
                        
                        // получаем тип запроса: 0 - показ, 1 - дефолт, 2 - клик
                        p_buf = get_type_request(buf, &type);
                        len = size_request - sizeof(type);

                        out_log.LGR_MDEBG("type request = %hu", type);

                        switch(type)
                        {
                            case REQUEST_SHOW:
                            {
                                show_t *show = new show_t(uc, &out_log);

                                // обработка входных данных
                                if (show->get_input_data(p_buf, len, type) == SHOW_OK)
                                {
                                    // передача данных треду
                                    out_log.LGR_MDEBG("input_show->push = %p", show);
                                    input_show->push((void *)show, &rc);
                                    assert_err(rc == 1);
                                    cnt[0]++;
                                }
                                else
                                {                   
                                    delete show;
                                }
                            }
                            break;

                            case REQUEST_DEFAULT_SHOW:
                            {
                                default_show_t *default_show = new default_show_t(uc, &out_log);

                                // обработка входных данных
                                if (default_show->get_input_data(p_buf, len, type) == SHOW_OK)
                                {
                                    // передача данных треду
                                    out_log.LGR_MDEBG("input_default_show->push = %p", default_show);
                                    input_default_show->push((void *)default_show, &rc);
                                    assert_err(rc == 1);
                                    cnt[1]++;
                                }
                                else
                                {                   
                                    delete default_show;
                                }
                            }
                            break;

                            case REQUEST_CLICK:
                            {
                                click_t *click = new click_t(uc, &out_log);

                                // обработка входных данных
                                if (click->get_input_data(p_buf, len) == CLICK_OK)
                                {
                                    // передача данных треду
                                    out_log.LGR_MDEBG("input_dclick->push = %p", click);
                                    input_click->push((void *)click, &rc);
                                    assert_err(rc == 1);
                                    cnt[2]++;
                                }
                                else
                                {
                                    delete click;
                                }                
                            }
                            break;
                        }
                        break;
                        
                        default:
                            out_log.LGR_MDEBG("Wrong source: %s", units[buff[recv_index].address]);
                 }
             }
             else
             {
                out_log.LGR_MDEBG("Wrong type: %d (source: %s)", buff[recv_index].type, units[buff[recv_index].address]);
             }
        }
    }

    // посылаем сигнал треду о завершении работы
    input_show->push((void *)NULL, &rc);
    assert_err(rc == 1);
    // ожидание завершения работы треда
    pthread_join(thr_show, NULL);
    
    // посылаем сигнал треду о завершении работы
    input_default_show->push((void *)NULL, &rc);
    assert_err(rc == 1);
    // ожидание завершения работы треда
    pthread_join(thr_default_show, NULL);
    
    // посылаем сигнал треду о завершении работы
    input_click->push((void *)NULL, &rc);
    assert_err(rc == 1);
    // ожидание завершения работы треда
    pthread_join(thr_click, NULL);
    
    // очистка памяти
    delete input_show;
    delete[] parameters.array;   
    
    delete input_default_show;
    delete[] parameters_default.array;  
    
    delete input_click;
    delete[] parameters_click.array;
    
    printf("%u %u %u\n", cnt[0], cnt[1], cnt[2]);
    
    return 0;
}



// обработка показа
void *thread_show(void *args)
{
    // получение указателей на входную очередь и лог
    parameters_t *parameters = (parameters_t *)args;
    if (parameters->count != 2)
    {
        return NULL;
    }
    M_queue *input = (M_queue *)(parameters->array[0]);
    Lgr *out_log = (Lgr *)(parameters->array[1]);

    // база данных
    const char database[] = "storage";
    pthread_mutex_lock(&init_mutex);
        db_t db(out_log, host, user, password, database);
    pthread_mutex_unlock(&init_mutex);
    // ошибка
    int rc, error;
    // показ
    show_t *show;
    
    // лог бюджета
    budget_log_t budget_log(out_log);
    
    for ( ;; )
    {
        // получаем указатель на показ
        show = (show_t *)input->pop(&rc);
        assert_err(rc == 1);
        
        out_log->LGR_MDEBG("input_show->pop = %p", show);
        
        if (show != NULL)
        {
            error = SHOW_OK;
            do
            {
                // информация о месте
                if (show->get_place_info(&db) != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error place info");
                    error = SHOW_ERROR;
                    break;
                }

                // код заявок
                if (show->code_show(&db) != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error code show");
                    error = SHOW_ERROR;
                    break;
                }

                // ответ в nginx
                if (show->send_answer_nginx() != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error send answer nginx");
                    error = SHOW_ERROR;
                    break;
                }
                
                // запись ТГ сворачиваний (нет в альфа версии)
                /*if (show->swerving_tg())
                {
                    show->save_swerving_tg(&db);
                }*/

                // передача в бюджет и запись в лог
                if (show->send_answer_budget(&budget_log) != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error send answer budget");
                    error = SHOW_ERROR;
                    break;
                }

                // запрос в статистику
                if (show->send_answer_statistics() != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error send answer statistics");
                    error = SHOW_ERROR;
                    break;
                }           
            } while (0);
            
            if (error == SHOW_ERROR)
            {
                // передаем ошибку в nginx
                if (show->send_error_answer_nginx() != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error send error answer nginx");
                }
            }
            
            delete show;
        }
        else
        {
            // завершение работы
            break;
        }
    }
    return NULL;
}



// обработка дефолтного показа
void *thread_default_show(void *args)
{
    // получение указателей на входную очередь и лог
    parameters_t *parameters = (parameters_t *)args;
    if (parameters->count != 2)
    {
        return NULL;
    }
    M_queue *input = (M_queue *)(parameters->array[0]);
    Lgr *out_log = (Lgr *)(parameters->array[1]);

    // база данных
    const char database[] = "storage";
    pthread_mutex_lock(&init_mutex);
        db_t db(out_log, host, user, password, database);
    pthread_mutex_unlock(&init_mutex);
    
    // ошибки
    int rc, error;
    // показ
    default_show_t *default_show;
    // тип дефолтного баннера
    default_show_t::type_default_answer_t type_banner;
       
    for ( ;; )
    {
        // получем указатель на показ
        default_show = (default_show_t *)input->pop(&rc);
        assert_err(rc == 1);
        
        out_log->LGR_MDEBG("input_default_show->pop = %p", default_show);
        
        if (default_show != NULL)
        {
            // сброс флага 
            error = SHOW_OK;
                
            do
            {
                // информация о месте
                if (default_show->get_place_info(&db) != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error place info (default)");
                    error = SHOW_ERROR;
                    break;
                }
                
                /*// получаем тип дефолтного баннера
                type_banner = default_show->get_type_banner();
                
                switch(type_banner)
                {
                    // сворачивание
                    case default_show_t::SWERVING:
                        
                        // создаем код сворачивания
                        if (default_show->code_swerving(&db) != SHOW_OK)
                        {
                            out_log->LGR_MCRIT("Error get code swerving (default)");
                            error = SHOW_ERROR;
                        }
                        break;
                        
                    // альтернативный баннер
                    case default_show_t::ALT_BANNER:
                        
                        // создаем код альтернативного баннера
                        if (default_show->code_alt_banner(&db) != SHOW_OK)
                        {
                            out_log->LGR_MCRIT("Error get code alt banner (default)");
                            error = SHOW_ERROR;
                        }
                        break;
                       
                    // заглушка
                    case default_show_t::STUB_BANNER:
                        */
                        // выбираем заглушку
                        if (default_show->code_stub(&db) != SHOW_OK)
                        {
                            out_log->LGR_MCRIT("Error get code stub (default)");
                            error = SHOW_ERROR;
                        }
                //        break;
                //}         
            } while (0);
            
            if (error != SHOW_ERROR)
            {
                // ответ в nginx
                if (default_show->send_answer_nginx() != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error send answer nginx (default)");
                }
            }
            else
            {
                // передаем ошибку в nginx
                if (default_show->send_error_answer_nginx() != SHOW_OK)
                {
                    out_log->LGR_MCRIT("Error send error answer nginx");
                }
            }
            
            delete default_show;
        }
        else
        {
            // завершение работы
            break;
        }
    }
    return NULL;
}



// обработка клика
void *thread_click(void *args)
{
    // получение указателей на входную очередь и лог
    parameters_t *parameters = (parameters_t *)args;
    if (parameters->count != 2)
    {
        return NULL;
    }
    M_queue *input = (M_queue *)(parameters->array[0]);
    Lgr *out_log = (Lgr *)(parameters->array[1]);

    // база данных
    const char database[] = "storage";
    pthread_mutex_lock(&init_mutex);
        db_t db(out_log, host, user, password, database);
    pthread_mutex_unlock(&init_mutex);
    
    // ошибка
    int rc, error;
    // клик
    click_t *click;
       
    for ( ;; )
    {
        // получаем указатель на клик
        click = (click_t *)input->pop(&rc);
        assert_err(rc == 1);
        
        out_log->LGR_MDEBG("input_click->pop = %p", click);
        
        if (click != NULL)
        {
            error = CLICK_OK;
            
            do
            {
                // выборка данных из БД 
                if (click->select_data(&db) != CLICK_OK)
                {
                    out_log->LGR_MCRIT("Error select data click");
                    error = CLICK_ERROR;
                    break;
                }
                
                // выдача ссылки nginx
                if (click->send_answer_nginx() != CLICK_OK)
                {
                    out_log->LGR_MCRIT("Error send answer nginx (click)");
                    error = CLICK_ERROR;
                    break;
                }
                
                // передача запроса в статистику
                if (click->send_answer_statistics() != CLICK_OK)
                {
                    out_log->LGR_MCRIT("Error send answer statistics (click)");
                    break;
                }
            } while (0);
            
            if (error == CLICK_ERROR)
            {
                // выдача ошибки nginx
                if (click->send_error_answer_nginx() != CLICK_OK)
                {
                    out_log->LGR_MCRIT("Error send error answer nginx (click)");
                }
            }
            
            delete click;
        }
        else
        {
            // завершение работы
            break;
        }
    }
    return NULL;
}



void set_log_global(Lgr *out_log)
{
#if 0    
#ifdef log_crit
    out_log->set_loglevel(LGR_EWARN, log_level_crit_global);
#endif 
#ifdef log_warn
    out_log->set_loglevel(LGR_EWARN, log_level_warn_global);
#endif    
#ifdef log_debug
    out_log->set_loglevel(LGR_EDEBG, log_level_debug_global);
#endif
#endif    
}



int test_show(char *buf, uint8_t type, Lgr *out_log)
{
    uint len = 0;
    
    // флаг ошибки
    buf[len++] = type;
    
    // длина адреса
    buf[len++] = 4;
    out_log->LGR_MDEBG("addr_len = %hu", 4);
    
    // адрес
    char *p_str = (char *)&out_log;
    memcpy(&buf[len], p_str, 4);
    len += 4;
    show_pointer(p_str, 4, out_log);
    
    // id места
    uint id_place = rand() % 50 + 1;
    memcpy(&buf[len], &id_place, 4);
    len += 4;
    out_log->LGR_MDEBG("id_place = %u", id_place);
    
    // записываем кук
    uint8_t new_cookie = (uint8_t)(rand() % 2);
    buf[len++] = new_cookie;
    char cookie[11];
    int *ms[3];
    ms[0] = (int *)cookie;
    *(ms[0]) = rand();
    ms[1] = (int *)(cookie + 4);
    *(ms[1]) = rand();
    ms[2] = (int *)(cookie + 7);
    *(ms[2]) = rand();
    memcpy(&buf[len], cookie, 11);
    len += 11;
    out_log->LGR_MDEBG("new cookie = %hu: cookie = %08x%05x%08x", new_cookie, *((uint32_t *)cookie),
        *((uint32_t *)(cookie + 4)), *((uint32_t *)(cookie + 7)));
    
    int i;
    // записываем заявки
    // количество
    uint8_t count = id_place % 8 + 1;
    out_log->LGR_MDEBG("count demands = %hu", count);
    memcpy(&buf[len], &count, 1);
    len += 1;
    uint id_campaign;
    uint16_t id_demands;
    uint8_t id_banner;
    uint cost;
    for (i = 0; i < count; ++i)
    {
        // кампания
        id_campaign = rand() % 50 + 1;
        memcpy(&buf[len], &id_campaign, 4);
        len += 4;
        
        // заявка
        id_demands = rand() % 20 + 1;
        memcpy(&buf[len], &id_demands, 2);
        len += 2;
        
        // баннер
        id_banner =  rand() % 8 + 1;
        memcpy(&buf[len], &id_banner, 1);
        len += 1;
        
        // стоимость
        cost = rand();
        memcpy(&buf[len], &cost, 4);
        len += 4;
        
        out_log->LGR_MDEBG("%d) %u %hu %hu %u", i, id_campaign,
            id_demands, id_banner, cost);
    }
    
    return len;
}



int test_click(char *buf, Lgr *out_log)
{
    uint len = 0;
    
    // флаг ошибки
    buf[len++] = 2;
    
    // длина адреса
    buf[len++] = 4;
    out_log->LGR_MDEBG("addr_len = %hu", 4);
    
    // адрес
    char *p_str = (char *)&out_log;
    memcpy(&buf[len], p_str, 4);
    len += 4;
    show_pointer(p_str, 4, out_log);
       
    // записываем кук
    uint8_t new_cookie = (uint8_t)(rand() % 2);
    buf[len++] = new_cookie;
    char cookie[11];
    int *ms[3];
    ms[0] = (int *)cookie;
    *(ms[0]) = rand();
    ms[1] = (int *)(cookie + 4);
    *(ms[1]) = rand();
    ms[2] = (int *)(cookie + 7);
    *(ms[2]) = rand();
    memcpy(&buf[len], cookie, 11);
    len += 11;
    out_log->LGR_MDEBG("new cookie = %hu: cookie = %08x%05x%08x", new_cookie, *((uint32_t *)cookie),
        *((uint32_t *)(cookie + 4)), *((uint32_t *)(cookie + 7)));
    
    // длина хеша
    uint8_t len_hash = 41;
    memcpy(&buf[len], &len_hash, 1);
    len += 1;
    
    // хеш
    char hash[50];
    char key_str[100];
    
    uint campaign = rand() % 50 + 1;
    uint16_t demand = rand() % 20 + 1;
    uint8_t banner = rand() % 7 + 1;
    uint place = rand() % 50 + 1;
    
    out_log->LGR_MDEBG("data: %u %u %hu %hu", place, campaign, demand, banner);
    
    uint64_t key = ((((uint64_t)campaign) << 32) + (((uint64_t)demand) << 16) + (uint64_t)banner);
    sprintf(key_str, "click_%u_%" PRIu64, place, key);
    hex_hash(key_str, hash);
    hash[40] = 0;
        
    memcpy(&buf[len], hash, len_hash);
    show_pointer(&buf[len], len_hash, out_log);
    len += len_hash;
    
    return len;
}



void show_pointer(char *str, int len, Lgr *out_log)
{
#ifndef log_debug
    return;
#else
    char buf[256];
    char *p = buf;
    int i;
    for (i = 0; i < len; i++)
        p += sprintf(p, "%02x", str[i] & 0xFF);
    p[0] = 0;
    out_log->LGR_MDEBG("address = 0x%s", buf);
#endif
}

char *get_type_request(char *buf, uint8_t *type)
{
    char *p_buf = buf;
    
    memcpy(type, p_buf, sizeof(*type));

    p_buf += sizeof(*type);
    
    return p_buf;
}



parameters_t set_parameters(M_queue *m, Lgr *log)
{
    parameters_t parameters;
    
    parameters.count = 2;
    
    parameters.array = new void *[parameters.count];
    assert_mem(parameters.array);
    
    parameters.array[0] = m;
    parameters.array[1] = log; 
    
    return parameters;
}



void hex_hash(char *str, char *dest)
{
    unsigned char str_out[100];
    char *p_res;
    int j;
    
    SHA1((unsigned char *) str, strlen(str), str_out);
    
    p_res = dest;
    // преобразование из числа в строку
    for(j = 0; j < SHA_DIGEST_LENGTH; ++j)
    {
        p_res[0] = st[str_out[j] / 16];
        p_res[1] = st[str_out[j] % 16];
        p_res += 2;
    }
    *p_res = 0;
}