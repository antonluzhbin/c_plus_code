/* 
 * File:   show.cpp
 * Author: anton
 * 
 * Created on 13 Июнь 2013 г., 12:07
 */

#include "show.h"

show_t::show_t(ucs_t *uc_, Lgr *log_)
        : base_show_t(uc_, log_)
{
}



show_t::show_t() 
{
}



show_t::show_t(const show_t& orig) 
{
}



show_t::~show_t() 
{
}


// считывание кода заявок для показа
int show_t::code_show(db_t *db)
{
    char str[1024];
    char *p_str = str;
    
    // формируем запрос
    p_str += sprintf(p_str, "SELECT PO1,PO5,PO7,PO8 from `show`"
                     " where PO0 = %u and PO1 IN (%" PRIu64, idt.id_place, idt.campaign[0].key());
    
    int i;
    for (i = 1; i < idt.count_campaign; ++i)
    {
        p_str += sprintf(p_str, ",%" PRIu64, idt.campaign[i].key());
    }
    
    p_str += sprintf(p_str, ")");
    
    log->LGR_MDEBG("%s", str);
    
    // запрос
    if (db->query(str) != DB_OK)
    {
        log->LGR_MCRIT("Error: %d, %s", db->last_errno(), db->last_error());
        return SHOW_ERROR;
    }
    
    // количество ошибок
    int error_count = 0;
    
    // ключ = id кампании, заявки и баннера
    uint64_t key;
    
    // суммарния длина кода всех баннеров
    idt.len_all_code = 0;
    
    int data_not_found = 1;
    
    // обработка ответа
    while (db->get_next_row())
    {
        // получаем данные
        key = my_strtouq(db->get_value(0), &error_count);
        
        // ошибка обработки данных
        if (error_count > 0)
        {
            db->free_result();
            log->LGR_MCRIT("Error parsing show data");
            return SHOW_ERROR;    
        }
               
        for (i = 0; i < idt.count_campaign; ++i)
        {
            // ищем баннер в массиве
            if (key == idt.campaign[i].key())
            {
                if (idt.campaign[i].code != NULL)
                {
                    db->free_result();
                    log->LGR_MCRIT("Error show data (double banners)");
                    return SHOW_ERROR; 
                }
                
                data_not_found = 0;
                        
                // записываем принятые данные
                idt.campaign[i].id_link = my_strtoul(db->get_value(1), &error_count);
                idt.campaign[i].type = my_strtoul8(db->get_value(2), &error_count);
                idt.campaign[i].code_len = strlen(db->get_value(3)) + 1;
                
                // копируем код
                idt.len_all_code += idt.campaign[i].code_len;
                if (idt.campaign[i].code_len > 0)
                {
                    idt.campaign[i].code = new char[idt.campaign[i].code_len];
                    assert_mem(idt.campaign[i].code);
                    memcpy(idt.campaign[i].code, db->get_value(3), idt.campaign[i].code_len);
                }
                else
                {
                    idt.campaign[i].code = NULL;
                }
                
                // ошибка обработки данных
                if (error_count > 0)
                {
                    db->free_result();
                    log->LGR_MCRIT("Error parsing show data");
                    return SHOW_ERROR;    
                }
                break;
            }
        }
    }
    
    // освобождение памяти из под запроса
    db->free_result();
    
    if (data_not_found == 1)
    {
        return SHOW_ERROR;
    }
    
    // сборка js кода
    if (build_js_code() != SHOW_OK)
    {
        return SHOW_ERROR;
    }
    
    return SHOW_OK;
}



// сборка js кода (для nginx)
int show_t::build_js_code()
{   
    output_data_nginx.code_data = NULL;
    char begin_js_code_str[1024];
    sprintf(begin_js_code_str, begin_js_code, idt.id_place);
    int len_js = sizeof(begin_js_code_str) + strlen(end_js_code) + idt.count_campaign * sizeof("<br>");

    // размер ответа nginx-у (размер js кода)
    output_data_nginx.code_data = new char[idt.len_all_code + len_js];
    assert_mem(output_data_nginx.code_data);
         
    char *p_str = output_data_nginx.code_data;

    // начальный код
    p_str += sprintf(p_str, "%s", begin_js_code_str);
    int i;
    // вставляем код в блок
    if (idt.campaign[0].code != NULL)
    {
        p_str += sprintf(p_str, "%s", idt.campaign[0].code);
    }
    for (i = 1; i < idt.count_campaign; ++i)
    {
        if (idt.campaign[i].code != NULL)
        {
                p_str += sprintf(p_str, "<br>%s", idt.campaign[i].code);
        }
    }
    // конечный код
    p_str += sprintf(p_str, "%s", end_js_code);
    
    // размер кода
    output_data_nginx.code_len = p_str - output_data_nginx.code_data + 1;
    
    return SHOW_OK;
}



// рассчет размера ответа в зависимости от типа
int show_t::size_answer(type_unit_t type)
{
    int size = 0;
    
    switch (type)
    {
        case NGINX:
            size = sizeof(output_data_nginx.addr_len) +         // адрес nginx (длина + адрес) 
                   output_data_nginx.addr_len +        
                   sizeof(output_data_nginx.error) +            // ошибка
                   sizeof(output_data_nginx.type_len) +         // тип ответа (длина + тип)
                    output_data_nginx.type_len +                
                   sizeof(output_data_nginx.code_len) +         // ответ (длина + код) 
                    output_data_nginx.code_len;                               
            break;
            
        case STATISTICS:
            size = sizeof(uint8_t) +                            // тип запроса
                   sizeof(idt.id_place) +                       // id места
                   sizeof(idt.new_cookie) + sizeof(idt.cookie) + // cookie 
                   sizeof(idt.count_campaign) +                 //кол-во заявок
                   idt.count_campaign * (
                   sizeof(campaign_t::id_campaign) +            // id кампании
                   sizeof(campaign_t::id_demand) +              // id заявки 
                   sizeof(campaign_t::id_banner) +              // id баннера
                   sizeof(campaign_t::id_link));                // id ссылки
            break;
            
        case BUDGET:
            size = sizeof(answer_budget_t::unix_time) +         // метка времени
                   sizeof(campaign_t::id_campaign) +            // id кампании
                   sizeof(campaign_t::id_demand) +              // id заявки 
                   sizeof(campaign_t::id_banner) +              // id баннера
                   sizeof(idt.id_place) +                       // id места
                   sizeof(campaign_t::cost) +                   // стоимость показа
                   sizeof(answer_budget_t::num_log);            // номер лог файла
            break;
            
       case BUDGET_LOG:
            size = sizeof(answer_budget_t::unix_time) +         // метка времени
                   sizeof(campaign_t::id_campaign) +            // id кампании
                   sizeof(campaign_t::id_demand) +              // id заявки 
                   sizeof(campaign_t::id_banner) +              // id баннера
                   sizeof(idt.id_place) +                       // id места
                   sizeof(campaign_t::cost) +                   // стоимость показа
                   sizeof(answer_budget_t::send_status);        // флаг ошибки
            break;
    }

    return size;
}



// отправка ответа в бюджет
int show_t::send_answer_budget(budget_log_t *budget_log)
{
    char *tmp_buf;
    int len;
    struct timeval tv;
       
    // получение временной метки
    gettimeofday((struct timeval *) &tv, (struct timezone *) NULL);   
    answer_budget.unix_time = tv.tv_sec;
         
    int index;
    for (index = 0; index < idt.count_campaign; ++index)
    {
        // получаем номер файла лога
        answer_budget.num_log = budget_log->get_num_log();
        
        //log->LGR_MINFO("answer_budget.num_log = %hu (%hu)", answer_budget.num_log, budget_log->get_num_log());
        
        // сборка ответа
        if (build_answer_budget(&tmp_buf, &len, &answer_budget, index) != SHOW_OK)
        {
            delete[] tmp_buf;
            tmp_buf = NULL;
            return SHOW_ERROR;
        }

        // отправка ответа
        answer_budget.send_status = SHOW_ERROR;
        answer_budget.send_status = (uint8_t)send_data(tmp_buf, len, MODULE_BUDGET); 

        // очистка памяти из под js кода переданного в nginx
        delete[] tmp_buf;
        tmp_buf = NULL;

        // формируем блок данных
        if (build_log_budget(&tmp_buf, &len, &answer_budget, index) != SHOW_OK)
        {
            delete[] tmp_buf;
            tmp_buf = NULL;
            return SHOW_ERROR;
        }

        // запись в лог
        budget_log->write(tmp_buf, len, log);
        
        // очистка памяти из под данных лога
        delete[] tmp_buf;
        tmp_buf = NULL;
    }
    
    return SHOW_OK;
}



// отправка показа в статистику
int show_t::send_answer_statistics()
{
    char *buf;
    int len;
    
    // сборка ответа
    if (build_answer_statistics(&buf, &len) != SHOW_OK)
    {
        return SHOW_ERROR;
    }
    
    // отправка ответа
    send_data(buf, len, MODULE_STATISTICS); 
    
    // очистка памяти
    delete[] buf;
    
    return SHOW_OK;
}



// сборка ответа для статистики
int show_t::build_answer_statistics(char **p_buf, int *p_len)
{
    // определение размера буфера
    int size = size_answer(STATISTICS);
    
    // выделение памяти
    *p_buf = new char[size];
    assert_mem(*p_buf);
    
    // запись в буфер
    int len = 0;
    
    // тип запроса
    uint8_t type_answer = 3;
    memcpy((*p_buf + len), &type_answer, sizeof(type_answer));
    len += sizeof(type_answer);
    log->LGR_MINFO("type_answer = %hu", type_answer);
    
    // id места
    memcpy((*p_buf + len), &idt.id_place, sizeof(idt.id_place));
    len += sizeof(idt.id_place);
    log->LGR_MINFO("id_place = %u", idt.id_place);
    
    // cookie 
    memcpy((*p_buf + len), &idt.new_cookie, sizeof(idt.new_cookie));
    len += sizeof(idt.new_cookie);
    
    memcpy((*p_buf + len), &idt.cookie, COOKIE_LENGTH);
    len += COOKIE_LENGTH;
    log->LGR_MINFO("new cookie = %hu: cookie = %08x%05x%08x", idt.new_cookie, *((uint32_t *)idt.cookie),
        *((uint32_t *)(idt.cookie + 4)), *((uint32_t *)(idt.cookie + 7)));
    
    //кол-во заявок 
    memcpy((*p_buf + len), &idt.count_campaign, sizeof(idt.count_campaign));
    len += sizeof(idt.count_campaign);
    log->LGR_MINFO("count campaign = %hu", idt.count_campaign);
    
    int i;
    for (i = 0; i < idt.count_campaign; ++i)
    {
        // id кампании
        memcpy((*p_buf + len), &idt.campaign[i].id_campaign, sizeof(campaign_t::id_campaign));
        len += sizeof(campaign_t::id_campaign);
        
        // id заявки 
        memcpy((*p_buf + len), &idt.campaign[i].id_demand, sizeof(campaign_t::id_demand));
        len += sizeof(campaign_t::id_demand);
        
        // id баннера
        memcpy((*p_buf + len), &idt.campaign[i].id_banner, sizeof(campaign_t::id_banner));
        len += sizeof(campaign_t::id_banner);
        
        // id ссылки
        memcpy((*p_buf + len), &idt.campaign[i].id_link, sizeof(campaign_t::id_link));
        len += sizeof(campaign_t::id_link);
        
        log->LGR_MINFO("%d)%u %hu %hu %u", i, idt.campaign[i].id_campaign, 
                idt.campaign[i].id_demand, idt.campaign[i].id_banner, idt.campaign[i].id_link);
    }    
    
    // длина данных
    *p_len = len; 
    
    return SHOW_OK;
}



// сборка ответа для бюджета
int show_t::build_answer_budget(char **p_buf, int *p_len, answer_budget_t *answer_budget, int index)
{   
    // определение размера буфера
    int size = size_answer(BUDGET);
    
    // выделение памяти
    *p_buf = new char[size];
    assert_mem(*p_buf);
    
    // запись в буфер
    int len = 0;
    
    // метка времени
    memcpy((*p_buf + len), &answer_budget->unix_time, sizeof(answer_budget_t::unix_time));
    len += sizeof(answer_budget_t::unix_time);
    log->LGR_MINFO("unix_time = %u", answer_budget->unix_time);
    
    // id кампании
    memcpy((*p_buf + len), &idt.campaign[index].id_campaign, sizeof(campaign_t::id_campaign));
    len += sizeof(campaign_t::id_campaign);
    log->LGR_MINFO("id_campaign = %u", idt.campaign[index].id_campaign);
    
    // id заявки 
    memcpy((*p_buf + len), &idt.campaign[index].id_demand, sizeof(campaign_t::id_demand));
    len += sizeof(campaign_t::id_demand);
    log->LGR_MINFO("id_demand = %hu", idt.campaign[index].id_demand);
    
    // id баннера
    memcpy((*p_buf + len), &idt.campaign[index].id_banner, sizeof(campaign_t::id_banner));
    len += sizeof(campaign_t::id_banner);
    log->LGR_MINFO("id_banner = %hu", idt.campaign[index].id_banner);
    
    // id места
    memcpy((*p_buf + len), &idt.id_place, sizeof(idt.id_place));
    len += sizeof(idt.id_place);
    log->LGR_MINFO("id_place = %u", idt.id_place);
    
    // стоимость показа
    memcpy((*p_buf + len), &idt.campaign[index].cost, sizeof(campaign_t::cost));
    len += sizeof(campaign_t::cost);
    log->LGR_MINFO("cost = %u", idt.campaign[index].cost);
    
    // номер лог файла
    memcpy((*p_buf + len), &answer_budget->num_log, sizeof(answer_budget_t::num_log));
    len += sizeof(answer_budget_t::num_log);
    log->LGR_MINFO("num_log = %hu", answer_budget->num_log);   
    
    // длина данных
    *p_len = len;
    
    return SHOW_OK;
}



// сборка даннях для записи в лог бюджета
int show_t::build_log_budget(char **p_buf, int *p_len, answer_budget_t *answer_budget, int index)
{
    // определение размера буфера
    int size = size_answer(BUDGET_LOG);
    
    // выделение памяти
    *p_buf = new char[size];
    assert_mem(*p_buf);
    
    // запись в буфер
    int len = 0;
    // метка времени
    memcpy((*p_buf + len), &answer_budget->unix_time, sizeof(answer_budget_t::unix_time));
    len += sizeof(answer_budget_t::unix_time);
    log->LGR_MINFO("unix_time = %u", answer_budget->unix_time);
    
   // id кампании
    memcpy((*p_buf + len), &idt.campaign[index].id_campaign, sizeof(campaign_t::id_campaign));
    len += sizeof(campaign_t::id_campaign);
    log->LGR_MINFO("id_campaign = %u", idt.campaign[index].id_campaign);
    
    // id заявки 
    memcpy((*p_buf + len), &idt.campaign[index].id_demand, sizeof(campaign_t::id_demand));
    len += sizeof(campaign_t::id_demand);
    log->LGR_MINFO("id_demand = %hu", idt.campaign[index].id_demand);
    
    // id баннера
    memcpy((*p_buf + len), &idt.campaign[index].id_banner, sizeof(campaign_t::id_banner));
    len += sizeof(campaign_t::id_banner);
    log->LGR_MINFO("id_banner = %hu", idt.campaign[index].id_banner);
    
    // id места
    memcpy((*p_buf + len), &idt.id_place, sizeof(idt.id_place));
    len += sizeof(idt.id_place);
    log->LGR_MINFO("id_place = %u", idt.id_place);
    
    // стоимость показа
    memcpy((*p_buf + len), &idt.campaign[index].cost, sizeof(campaign_t::cost));
    len += sizeof(campaign_t::cost);
    log->LGR_MINFO("cost = %u", idt.campaign[index].cost);
    
    // флаг ошибки
    memcpy((*p_buf + len), &answer_budget->send_status, sizeof(answer_budget_t::send_status));
    len += sizeof(answer_budget_t::send_status);
    log->LGR_MINFO("send_status = %hu", answer_budget->send_status);   
    
    // длина буфера
    *p_len = len;
    
    return SHOW_OK;
}



// проверка есть ли сворачивания ТГ группы
int show_t::swerving_tg()
{
    if (place_info.type_place == 1 && place_info.cout_place_block != idt.count_campaign)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}
                


// запись ТГ своричиваний
void show_t::save_swerving_tg(db_t *db)
{
    // количество сворачивания ТГ группы = количеству в блоке - количество выбранных
    save_default(db, base_show_t::SWERVING_TG, (place_info.cout_place_block - idt.count_campaign));
}