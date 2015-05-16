/* 
 * -lmysqlclient - однопоточный режим
 * -lmysqlclient_r - многопоточный режим
 */

#ifndef DB_H
#define	DB_H

#include <pthread.h>
#include <mysql/mysql.h>
#include <unistd.h>
#include <string.h>
#include "../logger_cpp/Lgr.h"
#include "../assert.h"

#define MYSQL_KOL_CONNECT 5                     // количество переподключений к mysql
#define MYSQL_SLEEP 10000                       // количество микросекунд между переподключениями

#define MYSQL_CONNECTION_ERROR 2002             // Message: Can't connect to local MySQL server through socket '%s' (%d) 
#define MYSQL_CONN_HOST_ERROR 2003              // Message: Can't connect to MySQL server on '%s' (%d)
#define MYSQL_SERVER_GONE_ERROR 2006            // Message: MySQL server has gone away
#define MYSQL_SERVER_HANDSHAKE_ERR 2012         // Message: Error in server handshake
#define MYSQL_SERVER_LOST 2013                  // Message: Lost connection to MySQL server during query
#define MYSQL_INVALID_CONN_HANDLE 2048          // Message: Invalid connection handle
#define MYSQL_SERVER_LOST_EXTENDED 2055         // Message: Lost connection to MySQL server at '%s', system error: %d
#define MYSQL_ALREADY_CONNECTED 2058            // Message: This handle is already connected. Use a separate handle for each connection.

#define DB_OK 0
#define DB_ERROR 1

// сообщение об ошибке при подключении к серверу
const char mysql_connection_error[] = "Can't connect to local MySQL server";
    
class db_t {
public:
    db_t(Lgr *out_log);
    db_t(char *host, char *user, char *password, char *database, Lgr *out_log);
    db_t(char *host, char *user, char *password, char *database, int port, Lgr *out_log);
    virtual ~db_t();
    
    // подключение к БД
    int select_db(char *db_name);
    // переподключение к серверу
    int reconnect(char *new_host, char *new_user, char *new_password, char *new_database);
    int reconnect(char *new_host, char *new_user, char *new_password, char *new_database, int new_port);
    
    // запрос
    int query(char *str_query);
    int query_update(char *str_query);
    
    // ошибки
    const char *last_error();
    uint last_errno();
    
    // количество рядов (строк) в ответе
    uint num_rows();
    // количество полей (столбцов) в ответе
    uint num_fields();
    // считывание следующего ряда (строки)
    int get_next_row();
    // получение значения поля
    const char* get_value(uint index);
    // освобождение памяти от запроса
    void free_result();
    
private:
    // лог
    Lgr *out_log;   
    // параметры подключения
    char *host;
    char *user;
    char *password;
    char *database;
    int port;
    // сокет
    MYSQL *sock;
    // результат запроса
    MYSQL_RES *result_query;
    // ряд запроса
    MYSQL_ROW row;
    // количество полей в ответе
    uint count_fields;
    
    // запрещенные конструкторы
    db_t();
    db_t(const db_t& orig);
    
    // копирование строки с выделение памяти под результат
    char *copy_str(char *src);
    // очистка памяти от параметров
    void delete_memory_parameters();
    
    // инициализация сокета mysql
    void sock_init();
    // подключение к серверу
    int connection();
    // закрытие соединения с БД
    void close_sock();
    // проверка является ли ошибка последнего запроса серверной (разрыв соединения и т.д.)
    int server_error();
    // проверка подключения к серверу
    int connected();
    // нет подключения к серверу
    int not_connected();
    // проверка результата запроса
    int not_result_query();
    
    // установка параметров
    void set_database(char *new_database);
    void set_host(char *new_host);
    void set_user(char *new_user);
    void set_password(char *new_password);    
    
    void set_parameters(char *host, char *user, char *password, char *database, int port);
};

#endif	/* DB_H */