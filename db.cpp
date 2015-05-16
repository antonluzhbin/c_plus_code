/* 
 * File:   db.cpp
 * Author: root
 * 
 * Created on 6 Июнь 2013 г., 11:59
 */

#include "db.h"

// параметры по умолчанию
char default_host[] = "default_host";
char default_user[] = "default_user";
char default_password[] = "default_password";
char default_database[] = "";


// конструкторы
db_t::db_t(Lgr *out_log) 
    : host(NULL), user(NULL), password(NULL), database(NULL), sock(NULL), 
      result_query(NULL), port(0), count_fields(0)
{
    // параметры по умолчанию
    set_parameters(NULL, NULL, NULL, NULL, 0);
    
    // лог
    this->out_log = out_log;
}



db_t::db_t(char *host, char *user, char *password, char *database, Lgr *out_log) 
    : host(NULL), user(NULL), password(NULL), database(NULL), sock(NULL), 
      result_query(NULL), port(0), count_fields(0)
{
    // параметры подключения
    set_parameters(host, user, password, database, 0);
    
    // лог
    this->out_log = out_log;
    
    // подключение к БД
    connection();
}



db_t::db_t(char *host, char *user, char *password, char *database, int port, Lgr *out_log) 
    : host(NULL), user(NULL), password(NULL), database(NULL), sock(NULL), 
      result_query(NULL), port(0), count_fields(0)
{
    // параметры подключения
    set_parameters(host, user, password, database, port);
    
    // лог
    this->out_log = out_log;
    
    // подключение к БД
    connection();
}


db_t::db_t() 
{
}


db_t::db_t(const db_t& orig) 
{
}


// инициализация сокета
void db_t::sock_init()
{  
    // закрытие предыдущего соединения
    close_sock();
    
    int kol_connect = MYSQL_KOL_CONNECT;        // количество переподключений
    // инициализация
    do
    {
        if (kol_connect != MYSQL_KOL_CONNECT)
        {
            usleep(MYSQL_SLEEP);
            out_log->LGR_MWARN("mysql reinit");
        }
        sock = mysql_init(NULL);
        --kol_connect;
    } while (!sock && kol_connect >= 0);
}


// подключение к БД
int db_t::connection()
{    
    // инициализация сокета
    sock_init();
    if (not_connected())
    {
        return DB_ERROR;
    }

    int kol_connect = MYSQL_KOL_CONNECT;
    int fl = 0;
    // подключение
    do
    {
        if (kol_connect != MYSQL_KOL_CONNECT)
        {
            usleep(MYSQL_SLEEP);
            out_log->LGR_MWARN("mysql reconnect: '%d' - '%s'", mysql_errno(sock), mysql_error(sock));
        }
        fl = (mysql_real_connect(sock, host, user, password, database, port, NULL, 0) != NULL);
        --kol_connect;
    } while (!fl && kol_connect >= 0);

    // ошибка подключения
    if (!fl)
    {
        mysql_close(sock);
        sock = NULL;
        return DB_ERROR;
    }
    
    return DB_OK;
}


// копирование строки с выделение памяти под результат
char *db_t::copy_str(char *src)
{
    if (src == NULL)
    {
        return NULL;
    }
    
    // длина
    int len = strlen(src);
    if (len == 0)
    {
        return NULL;
    }
    
    // выделение памяти
    char *dest = new char [len + 1];
    assert_mem(dest);
    
    // копирование
    strcpy(dest, src);   
    
    return dest;
}


// деструктор
db_t::~db_t() 
{
    // очищаем память от параметров
    delete_memory_parameters();
    
    // очищаем память от результата запроса
    free_result();
    
    // закрытие сокета
    close_sock();  
    
    // закрываем поток
    my_thread_end();
}


// очистка памяти от параметров
void db_t::delete_memory_parameters()
{
    // очистка памяти
    if (host != NULL)
    {
        delete[] host;
        host = NULL;
    }
    
    if (user != NULL)
    {
        delete[] user;
        user = NULL;
    }
    
    if (password != NULL)
    {
        delete[] password;
        password = NULL;
    }
    
    if (database != NULL)
    {
        delete[] database;
        database = NULL;
    }
}


// очистка памяти от результатов запроса
void db_t::free_result()
{
    if (result_query != NULL)
    {
        mysql_free_result(result_query);
        result_query = NULL;
    }
}


// закрытие соединения с БД
void db_t::close_sock()
{
    if (connected())
    {
        mysql_close(sock);
        sock = NULL;
    }
}


// установка имени сервера
void db_t::set_host(char *new_host)
{
    // удаляем старые данные
    if (host != NULL)
    {
        delete[] host;
    }
    
    // установка
    if (new_host != NULL)
    {
        host = copy_str(new_host);
    }
    else
    {
        host = copy_str(default_host);
    }
}


// установка имени пользователя
void db_t::set_user(char *new_user)
{
    // удаляем старые данные
    if (user != NULL)
    {
        delete[] user;
    }
    
    // установка
    if (new_user != NULL)
    {
        user = copy_str(new_user);
    }
    else
    {
        user = copy_str(default_user);
    }
}


// установка пароля
void db_t::set_password(char *new_password)
{
    // удаляем старые данные
    if (password != NULL)
    {
        delete[] password;
    }
    
    // установка
    if (new_password != NULL)
    {
        password = copy_str(new_password);
    }
    else
    {
        password = copy_str(default_password);
    }
}


// установка имени БД
void db_t::set_database(char *new_database)
{
    // удаляем старые данные
    if (database != NULL)
    {
        delete[] database;
    }
    
    // установка
    if (new_database != NULL)
    {
        database = copy_str(new_database);
    }
    else
    {
        database = copy_str(default_database);
    }
}


// установка всех параметров
void db_t::set_parameters(char *host, char *user, char *password, char *database, int port)
{
    set_host(host);
    set_user(user);
    set_password(password);
    set_database(database);
    this->port = port;
}


// переподключение
int db_t::reconnect(char *new_host, char *new_user, char *new_password, char *new_database)
{   
    // установка новых параметров
    set_parameters(new_host, new_user, new_password, new_database, 0);
    
    // очищаем память от результата запроса
    free_result();
    
    // переподключение
    return connection();
}


// переподключение
int db_t::reconnect(char *new_host, char *new_user, char *new_password, char *new_database, int new_port)
{   
    // установка новых параметров
    set_parameters(new_host, new_user, new_password, new_database, new_port);
    
    // очищаем память от результата запроса
    free_result();    
    
    // переподключение
    return connection();
}


// проверка на разрыв соединения
int db_t::server_error()
{
    switch(mysql_errno(sock))
    {
        case MYSQL_SERVER_GONE_ERROR:
        case MYSQL_CONNECTION_ERROR: 
        case MYSQL_CONN_HOST_ERROR:
        case MYSQL_SERVER_HANDSHAKE_ERR:
        case MYSQL_SERVER_LOST:
        case MYSQL_INVALID_CONN_HANDLE:
        case MYSQL_SERVER_LOST_EXTENDED:
        case MYSQL_ALREADY_CONNECTED:
            return 1;
    }
    
    return 0;
}


// выбор БД
int db_t::select_db(char *db_name)
{
    int res = DB_ERROR;
    
    // новая БД
    set_database(db_name);
    out_log->LGR_MDEBG("mysql select DB: %s", database);

    // выбор БД
    res = mysql_select_db(sock, database);
    if (res == 1)
    {
        if (server_error())
        {
            // переподключение
            return connection();
        }
        else
        {
            return DB_ERROR;
        }
    }
    
    return DB_OK;
}


// проверка подключения к серверу
int db_t::connected()
{
    if (sock == NULL)
    {
        return 0;
    }
    
    return 1;
}


// нет подключения к серверу
int db_t::not_connected()
{
    if (sock == NULL)
    {
        return 1;
    }
    
    return 0;
}


// выполнение запроса
int db_t::query(char *str_query)
{
    // очищаем память от результата предыдущего запроса
    free_result();
    
    // запрос
    int state = DB_ERROR;
    if (connected())
    {
        state = mysql_query(sock, str_query);
    }

    // проверка на ошибки
    if(state != DB_OK)
    {
        if (not_connected() || server_error())
        {
            // переподключение при ошибке
            if (connection() == DB_OK)
            {
                // выполняем запрос повторно
                state = mysql_query(sock, str_query);
            }
        }

        // ошибка в запросе
        if( state != DB_OK )
        {
            result_query = NULL;
            return DB_ERROR;
        }
    }

    // считываем результат запроса
    result_query = mysql_store_result(sock);
    // проверка результата
    if (not_result_query())
    {
        return DB_ERROR;
    }
    
    return DB_OK;
}


// проверка результата запроса
int db_t::not_result_query()
{
    if (result_query == NULL)
    {
        return 1;
    }
    else
    {
        return 0;
    }    
}


// выполнение обновления
int db_t::query_update(char *str_query)
{
    // запрос
    int state = DB_ERROR;
    if (connected())
    {
        state = mysql_query(sock, str_query);
    }

    // проверка на ошибки
    if(state != DB_OK)
    {
        if (not_connected() || server_error())
        {
            // переподключение при ошибке
            if (connection() == DB_OK)
            {
                // выполняем запрос повторно
                state = mysql_query(sock, str_query);
            }
        }
    }
    
    return state;
}


// строка ошибка последнего запроса
const char *db_t::last_error()
{
    if (not_connected())
    {
        // сервер не подключен 
        return mysql_connection_error;
    }
    
    return mysql_error(sock);
}
    

// номер ошибки последнего запроса
uint db_t::last_errno()
{
    if (not_connected())
    {
        // сервер не подключен
        return 2002;
    }
    
    return mysql_errno(sock);
}


// количество рядов (строк) в ответе
uint db_t::num_rows()
{
    if (not_result_query())
    {
        return 0;
    }
    
    return mysql_num_rows(result_query);
}


// количество полей (столбцов) в ответе
uint db_t::num_fields()
{
    if (not_result_query())
    {
        return 0;
    }
    
    return mysql_num_fields(result_query);
}


// считывание строки результата запроса
int db_t::get_next_row()
{
    if (not_result_query())
    {
        return 0;
    }

    count_fields = mysql_num_fields(result_query);
    
    row = mysql_fetch_row(result_query);
    if (row == NULL)
    {
        return 0;
    }
    
    return 1;
}


// получение поля
const char* db_t::get_value(uint index)
{
    if (index < count_fields)
    {
        return row[index];
    }
    
    return NULL;
}