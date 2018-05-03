
/* Standard C++ includes */
#include <stdlib.h>
#include <iostream>

/*
  Include directly the different
  headers from cppconn/ and mysql_driver.h + mysql_util.h
  (and mysql_connection.h). This will reduce your build time!
*/
#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include "string_format.h"
#include <getopt.h>
#include "index.h"

using namespace std;

bool debug_=false;
std::string ext_;

void print_usage(const std::string& n){
    std::cout << "Usage: " << n
              << " -d <database> -t <tablename> --serv1 <server1> --user1 <username1> --pass1 <pass1>"
              << " --serv2 <server2> --user2 <username2> --pass1 <pass2>" << std::endl;
    return;
}
int main(int argc, char *argv[])
{
    std::string db,table,u1,u2,p1,p2,s1,s2;
    int c;
    int option_index;
    while (1) {
        int this_option_optind = optind ? optind : 1;
        int option_index = 0;
        static struct option long_options[] = {
                {"database", required_argument, NULL,  'd' },
                {"table",    required_argument, NULL,  't' },
                {"user1",    required_argument, NULL,  'u' },
                {"pass1",    required_argument, NULL,  'p' },
                {"user2",    required_argument, NULL,  122 },
                {"pass2",    required_argument, NULL,  123 },
                {"serv1",    required_argument, NULL,  124 },
                {"serv2",    required_argument, NULL,  125 },
                {"ext",    required_argument, NULL,  126 },
                {"debug",    optional_argument, NULL,  127 },
                {NULL,         0,                 NULL,  0 }
        };

        c = getopt_long(argc, argv, "d:t:u:p:",
                        long_options, &option_index);
        if (c == -1)
            break;

        switch (c) {
            case 0:
                printf("option %s", long_options[option_index].name);
                if (optarg)
                    printf(" with arg %s", optarg);
                printf("\n");
                break;

            case 'd':
                db = optarg;
                break;
            case 't':
                table = optarg;
                break;
            case 'u':
                u1 = optarg;
                break;
            case 'p':
                p1 = optarg;
                break;
            case 122:
                u2 = optarg;
                break;
            case 123:
                p2 = optarg;
                break;
            case 124:
                s1 = optarg;
                break;
            case 125:
                s2 = optarg;
                break;
            case 126:
                ext_ = optarg;
                break;
            case 127:
                debug_ = true;
                break;

            case '?':
                break;

            default:
                printf("?? getopt returned character code 0%o ??\n", c);
        }
    }

    if (optind < argc) {
        printf("non-option ARGV-elements: ");
        while (optind < argc)
            printf("%s ", argv[optind++]);
        printf("\n");
    }

    if (s1.size() == 0 or
        s2.size() == 0 or
        db.size() == 0 or
        table.size() ==0 or
        u1.size() == 0 or
        p1.size() == 0 ) {
        print_usage(argv[0]);
        exit(1);
    }

    if (u2.size() == 0){ u2=u1; }
    if (p2.size() == 0){ p2=p1; }

    set_global(debug_,ext_);

    //End options


    my::index idx1(db,table);
//    my::index idx2(db,table);
    try {
        sql::Driver *driver;
        sql::Connection *con, *con2;
        sql::Statement *stmt;
        sql::ResultSet *res;

        /* Create a connection */
        driver = get_driver_instance();
        con = driver->connect("tcp://"+s1+":3306", u1, p1);
        con2 = driver->connect("tcp://"+s2+":3306", u2, p2);
        /* Connect to the MySQL test database */
        con->setSchema(db);
        con2->setSchema(db);

        idx1.init_index(con);
        compare_tables_iterator(con,con2,idx1);

//        tt_.table_stat_init(con);
//        tt2_.table_stat_init(con2);
//        index_range irange = tt_.init_index_range();
//        compare_tables(con,con2,tt_,tt2_,irange);
    }
    catch (sql::SQLException &e) {
        cerr << "# ERR: SQLException in " << __FILE__;
        cerr << "(" << __FUNCTION__ << ") on line "
        << __LINE__ << endl;
        cerr << "# ERR: " << e.what();
        cerr << " (MySQL error code: " << e.getErrorCode();
        cerr << ", SQLState: " << e.getSQLState() << " )" << endl;
    }


//    tt_.print();
//    tt2_.print();

    return EXIT_SUCCESS;
}