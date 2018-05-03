//
// Created by dro on 02.05.18.
//
#pragma once
#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <vector>
#include <string>
#include <memory>

#include "string_format.h"
#include "checksum.h"

#define MAX_STEP 65280
#define MIN_STEP 255

void set_global(bool debug_, std::string ext_);


namespace my {

    class idx_std_field {
    public:
        idx_std_field(std::string name, std::string type)
                : name_(name), start_(0), stop_(0), cardinality_(0), type_(type) {};

        idx_std_field(std::string name, int64_t cardinality, std::string type)
                : name_(name), start_(0), stop_(0), cardinality_(cardinality), type_(type) {};

        idx_std_field(std::string name, int64_t start, int64_t stop, int64_t cardinality, std::string type)
                : name_(name), start_(start), stop_(stop), cardinality_(cardinality), type_(type) {};

        virtual std::string get_sql_where() const = 0;

        virtual std::string get_sql_where(int64_t min, int64_t max) const = 0;

        virtual std::string get_sql_sel_min_max() const = 0;

        int64_t start_;
        int64_t stop_;
        int64_t cardinality_;
        std::string name_;
        std::string type_;
    };

    class idx_int_field : public idx_std_field {
    public:
        idx_int_field(std::string name)
                : idx_std_field(name, "int") {};

        idx_int_field(std::string name, int64_t cardinality)
                : idx_std_field(name, cardinality, "int") {};

        idx_int_field(std::string name, int64_t start, int64_t stop, int64_t cardinality)
                : idx_std_field(name, start, stop, cardinality, "int") {};

        std::string get_sql_where() const;

        std::string get_sql_where(int64_t min, int64_t max) const;

        std::string get_sql_sel_min_max() const;
    };

    class idx_date_field : public idx_std_field {
    public:
        idx_date_field(std::string name)
                : idx_std_field(name, "date") {};

        idx_date_field(std::string name, int64_t cardinality)
                : idx_std_field(name, cardinality, "date") {};

        idx_date_field(std::string name, int64_t start, int64_t stop, int64_t cardinality)
                : idx_std_field(name, start, stop, cardinality, "date") {};

        std::string get_sql_where() const;

        std::string get_sql_where(int64_t min, int64_t max) const;

        std::string get_sql_sel_min_max() const;
    };

    struct index_all {
        std::string name;
        size_t position;
        std::string type;
    };

    struct column {
        std::string name;
        size_t position;
        std::string type;
        std::string charset_name;
        std::string collation_name;
    };


    class index {
    public:
        index(const std::string &dbname, const std::string &tablename) : tablename_(tablename), dbname_(dbname) {}

        void init_index(sql::Connection *con);

        void parse_columns(sql::Connection *con); //Get columns from table
        void parse_index(sql::Connection *con); // Get index statistics from table
        void parse_index_min_max(sql::Connection *con, size_t index_num);
        index copy_index() const;

        std::string get_sql_where() const;
        std::string get_dbname() const;
        std::string get_tablename() const;
        std::string get_col_names() const;
        std::vector<std::string> get_colls() const;
        std::string get_full_index_names() const;
        std::vector<std::string> get_full_index() const;
        std::string get_start_stop() const;

        void index_iteration_first_step(index &irange);
        bool index_iteration_can_next_step(const index &irange);
        void index_iteration_next_step(index &irange);

        bool index_can_split() const;
        std::pair<index,index> index_split(sql::Connection *con,const index& idx);

    private:
        std::vector<std::shared_ptr<idx_std_field>> Range;
        std::vector<index_all> all_indexes_;
        std::vector<column> all_colls_;
        int64_t index_count = 0;
        int64_t index_ranged = 0;
        const std::string tablename_;
        const std::string dbname_;
    };
}

struct field {
    std::string name;
    std::string value;
};



struct checksum_ {
    int64_t cnt;
    std::string crc;
};

checksum_ checksum(sql::Connection *con, const my::index& idx);

void compare_tables_iterator(sql::Connection *con, sql::Connection *con2, my::index& idx);

void compare_tables(sql::Connection *con, sql::Connection *con2, const my::index& irange);

std::map<std::vector<field>,std::string>
rows_checksum(sql::Connection *con,const my::index& idx);

std::vector<field>
get_all_line(sql::Connection *con,const my::index& idx);



bool operator== (checksum_ lhs, checksum_ rhs) ;
bool operator!= (checksum_ lhs, checksum_ rhs) ;
bool operator< (field lhs, field rhs) ;
