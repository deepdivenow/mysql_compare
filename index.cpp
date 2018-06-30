//
// Created by dro on 02.05.18.
//

#include "index.h"

bool DEBUG=false;
std::string EXT;

void set_global(bool debug_, std::string ext_){
    EXT=ext_;
    DEBUG=debug_;
}

using namespace my;

std::string idx_int_field::get_sql_where() const{
    return boost::str(boost::format("`%s` between %d AND %d") % name_ % start_ % stop_);
}

std::string idx_date_field::get_sql_where() const{
    return boost::str(boost::format("`%s` between FROM_DAYS(%d) AND FROM_DAYS(%d)") % name_ % start_ % stop_);
}

std::string idx_int_field::get_sql_where(int64_t min, int64_t max) const{
    return boost::str(boost::format("`%s` between %d AND %d") % name_ % min % max);
}

std::string idx_date_field::get_sql_where(int64_t min, int64_t max) const{
    return boost::str(boost::format("`%s` between FROM_DAYS(%d) AND FROM_DAYS(%d)") % name_ % min % max);
}

std::string idx_int_field::get_sql_sel_min_max() const{
    return boost::str(boost::format("min(%1%) AS MIN ,max(%1%) AS MAX") % name_);
}

std::string idx_date_field::get_sql_sel_min_max() const{
    return boost::str(boost::format("TO_DAYS(min(%1%)) AS MIN, TO_DAYS(max(%1%)) AS MAX") % name_);
}

void index::init_index(sql::Connection *con) {
    parse_columns(con);
    parse_index(con);
    parse_index_min_max(con, 0);
}

void index::parse_columns(sql::Connection *con){  //Get columns from table
    if (DEBUG) { std::cerr << "DB:" << dbname_ << " TBL:" <<  tablename_ << std::endl; }
    std::string sql=boost::str(boost::format("SELECT COLUMN_NAME,ORDINAL_POSITION,DATA_TYPE,CHARACTER_SET_NAME,COLLATION_NAME "
                                             "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' "
                                             "ORDER BY ORDINAL_POSITION;") % dbname_ % tablename_);

    if (DEBUG) { std::cerr << sql << std::endl; }
    auto stmt = con->createStatement();
    auto res = stmt->executeQuery(sql);
    all_colls_.clear();
    while (res->next()) {
        column tmp;
        tmp.name = res->getString("COLUMN_NAME");
        tmp.position = res->getInt("ORDINAL_POSITION");
        tmp.type = res->getString("DATA_TYPE");
        tmp.charset_name = res->getString("CHARACTER_SET_NAME");
        tmp.collation_name = res->getString("COLLATION_NAME");
        all_colls_.push_back(tmp);
    }
    delete res;
    delete stmt;
}

void index::parse_index(sql::Connection *con) { // Get index statistics from table

    std::string sql = boost::str(boost::format("SELECT COLUMN_NAME,SEQ_IN_INDEX,DATA_TYPE,ROUND(1000*TABLE_ROWS/CARDINALITY,0) AS CARDI "
                                               "FROM INFORMATION_SCHEMA.STATISTICS "
                                               "JOIN INFORMATION_SCHEMA.COLUMNS USING(TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME) "
                                               "JOIN INFORMATION_SCHEMA.TABLES USING(TABLE_SCHEMA,TABLE_NAME) "
                                               "WHERE INDEX_NAME = 'PRIMARY' AND TABLE_SCHEMA='%s' "
                                               "AND TABLE_NAME='%s' "
                                               "ORDER BY SEQ_IN_INDEX;") % dbname_ % tablename_);
    if (DEBUG) { std::cerr << sql << std::endl; }
    auto stmt = con->createStatement();
    auto res = stmt->executeQuery(sql);
    Range.clear();
    while (res->next()) { // Push index fields by type
        std::string tmp_name = res->getString("COLUMN_NAME");
        size_t tmp_position = res->getInt("SEQ_IN_INDEX");
        std::string tmp_type = res->getString("DATA_TYPE");
        int64_t tmp_cardinality = res->getInt("CARDI");
        all_indexes_.push_back({tmp_name,tmp_position,tmp_type});
        if (tmp_type == "int" or tmp_type == "bigint") {
            Range.push_back(std::make_shared<idx_int_field>(tmp_name,tmp_cardinality));
            index_count++;
        } else if (tmp_type == "date" or tmp_type == "datetime") {
            Range.push_back(std::make_shared<idx_date_field>(tmp_name,tmp_cardinality));
            index_count++;
        } else {
            continue;
        }
    }
    delete res;
    delete stmt;
}

std::string index::get_dbname() const{
    return dbname_;
}
std::string index::get_tablename() const{
    return tablename_;
}

std::string index::get_col_names() const{
    std::string ret;
    for (size_t i=0;i<all_colls_.size();i++){
        ret+="`"+all_colls_[i].name+"`";
        if (i < all_colls_.size() - 1){
            ret+=", ";
        }
    }
    return ret;
}

std::string index::get_full_index_names() const {
    std::string ret;
    for (size_t i=0;i<all_indexes_.size();i++){
        ret+="`"+all_indexes_[i].name+"`";
        if (i < all_indexes_.size() - 1){
            ret+=", ";
        }
    }
    return ret;
}

std::vector<std::string> index::get_full_index() const {
    std::vector<std::string> ret;
    for (const auto& v : all_indexes_){
        ret.push_back(v.name);
    }
    return ret;
}

std::vector<std::string> index::get_colls() const {
    std::vector<std::string> ret;
    for (const auto& v : all_colls_){
        ret.push_back(v.name);
    }
    return ret;
}

std::string index::get_sql_where() const{
    //GET WHERE FROM ALL RANGED INDEXES
    std::string where;
    for (int i = 0; i < index_ranged; i++) {
        where += Range[i]->get_sql_where();
        if (i < index_ranged - 1) {
            where += " AND ";
        }
    }
    if (where.size() == 0){
        where = "TRUE";
    }
    return where;
}

std::string index::get_start_stop() const {
    std::string r1,r2;
    for (size_t i=0;i<index_ranged;i++) {
        r1+=std::to_string(Range[i]->start_);
        r2+=std::to_string(Range[i]->stop_);
        if (i < index_ranged - 1) {
            r1+=":";
            r2+=":";
        }
    }
    return r1+"/"+r2;
}


void index::parse_index_min_max(sql::Connection *con,size_t index_num) {
    if (index_num > index_count || index_ranged != index_num) {
        std::cerr << "Bad index min/max parse:"<< index_num << std::endl;
        exit(11);
    }

    std::string sql=boost::str(boost::format(
            "SELECT %s FROM `%s`.`%s` FORCE INDEX (`PRIMARY`) WHERE %s")
            % Range[index_num]->get_sql_sel_min_max() % dbname_ % tablename_ % get_sql_where());

    if(EXT.size()>0){
        sql+=" AND "+EXT;
    }
    if (DEBUG) { std::cerr << sql << std::endl; }
    auto stmt = con->createStatement();
    auto res = stmt->executeQuery(sql);
    while (res->next()) {
        Range[index_num]->start_ = res->getInt("MIN");
        Range[index_num]->stop_ = res->getInt("MAX");
    }
    delete res;
    delete stmt;

    if (DEBUG) { std::cerr << "# Start: " << Range[index_num]->start_ << " Stop:" << Range[index_num]->stop_ << std::endl; }

    ///Calc cardinality
    if (index_num != 0){

        std::string sql=boost::str(boost::format(
                "SELECT count(*) AS COUNT FROM `%s`.`%s` FORCE INDEX (`PRIMARY`) WHERE %s")
                % dbname_ % tablename_ % get_sql_where());

        if (DEBUG) { std::cerr << sql << std::endl; }
        auto stmt = con->createStatement();
        auto res = stmt->executeQuery(sql);
        int64_t cols1p_real;
        while (res->next()) {
            cols1p_real = res->getInt("COUNT");
        }
        int64_t cols1p_byindex=Range[index_num]->stop_-Range[index_num]->start_+1;
        if (cols1p_byindex == 0) { cols1p_byindex = 1; }
        Range[index_num]->cardinality_ = cols1p_real*CARDI_RANGE / cols1p_byindex;
        if (Range[index_num]->cardinality_ < 1){
            Range[index_num]->cardinality_=1;
        }
        if (DEBUG) { std::cerr << "# Cardinality min/max: " << Range[index_num]->cardinality_ << std::endl; }
        delete res;
        delete stmt;
    }
    index_ranged++;
}

my::index index::copy_index() const{
    my::index ret(dbname_,tablename_);
    ret.index_ranged = index_ranged;
    ret.all_colls_ = all_colls_;
    ret.all_indexes_ = all_indexes_;
    ret.index_count = index_count;
    ret.index_ranged = index_ranged;
    for (size_t i=0; i<Range.size(); i++){
        if (Range[i]->type_ == "int"){
            ret.Range.push_back(std::make_shared<idx_int_field>(Range[i]->name_,Range[i]->cardinality_));
        } else if (Range[i]->type_ == "date"){
            ret.Range.push_back(std::make_shared<idx_date_field>(Range[i]->name_,Range[i]->cardinality_));
        }
    }
    for (size_t i=0; i<index_ranged; i++){
        ret.Range[i]->start_=Range[i]->start_;
        ret.Range[i]->stop_=Range[i]->stop_;
    }
    return ret;
}

void index::index_iteration_first_step(index& irange){
    int ii = MAX_STEP*CARDI_RANGE / Range[0]->cardinality_;
    if (ii < 1) { ii=1; }
    if (ii > MAX_STEP*CARDI_RANGE) { ii=MAX_STEP*CARDI_RANGE; }
    if (DEBUG) { std::cerr << "#ii: "<< ii << std::endl; }
    irange.Range[0]->stop_=irange.Range[0]->start_+ii;
}

bool index::index_iteration_can_next_step(const index& irange){
    if (irange.Range[0]->start_<=irange.Range[0]->stop_){
        return true;
    } else {
        return false;
    }
}

void index::index_iteration_next_step(index& irange){
    int ii = MAX_STEP*CARDI_RANGE / Range[0]->cardinality_;
    if (ii < 1) { ii=1; }
    if (ii > MAX_STEP*CARDI_RANGE) { ii=MAX_STEP*CARDI_RANGE; }
    irange.Range[0]->start_ = irange.Range[0]->stop_+1;
    irange.Range[0]->stop_ = irange.Range[0]->start_ + ii;
    if (irange.Range[0]->stop_ > Range[0]->stop_){
        irange.Range[0]->stop_=Range[0]->stop_;
    }
}

bool index::index_can_split() const{
    size_t i = index_ranged-1;
    if ( Range[i]->start_<Range[i]->stop_ || index_ranged < index_count ) {
        if (Range[i]->cardinality_*(Range[i]->stop_-Range[i]->start_+1) > MIN_STEP*CARDI_RANGE){
            if (DEBUG) { std::cerr << "#split: "<< Range[i]->cardinality_*(Range[i]->stop_-Range[i]->start_+1) << '>'
                                   << MIN_STEP*CARDI_RANGE << std::endl; }
            return true;
        }
    }
    return false;
}

std::pair<my::index,my::index> index::index_split(sql::Connection *con, const index& idx){
    my::index ir1=idx.copy_index();
    my::index ir2=idx.copy_index();
    for (int i = 0; i < index_ranged; i++) {
        if (Range[i]->start_ < Range[i]->stop_) {
            int64_t slice = (Range[i]->stop_ - Range[i]->start_) / 2;
            ir1.Range[i]->stop_=ir1.Range[i]->start_+slice;
            ir2.Range[i]->start_=ir1.Range[i]->stop_+1;
            return std::make_pair(ir1,ir2);
        }
    }
    //Create new range if we can
    if (index_ranged<index_count) {
        ir1.parse_index_min_max(con, index_ranged);
        ir2.parse_index_min_max(con, index_ranged);
        int64_t slice = (ir1.Range[index_ranged]->stop_ - ir1.Range[index_ranged]->start_) / 2;
        ir1.Range[index_ranged]->stop_=ir1.Range[index_ranged]->start_+slice;
        ir2.Range[index_ranged]->start_=ir1.Range[index_ranged]->stop_+1;
        return std::make_pair(ir1,ir2);
    }
        std::cerr << "Bad split" << std::endl;
        exit(15);
}

bool operator== (checksum_ lhs, checksum_ rhs) {
    return lhs.crc == rhs.crc && lhs.cnt == rhs.cnt;
}

bool operator!= (checksum_ lhs, checksum_ rhs) {
    return lhs.crc != rhs.crc || lhs.cnt != rhs.cnt;
}

checksum_ checksum(sql::Connection *con, const my::index& idx){

    std::string sql=boost::str(boost::format("SELECT 0 AS chunk_num, COUNT(*) AS cnt, "
            "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#',%s)) AS UNSIGNED)), 10, 16)), 0) AS crc "
            "FROM `%s`.`%s` FORCE INDEX (`PRIMARY`) WHERE %s ")
            % idx.get_col_names() % idx.get_dbname() % idx.get_tablename() % idx.get_sql_where());

    if(EXT.size()>0){
        sql+=" AND "+EXT;
    }
    if (DEBUG) { std::cerr << sql << std::endl; }
    auto stmt = con->createStatement();
    auto res = stmt->executeQuery(sql);
    checksum_ ret;
    while (res->next()) {
        ret.cnt = res->getInt("cnt");
        ret.crc = res->getString("crc");
    }
    delete res;
    delete stmt;
    return ret;
}

bool operator< (field lhs, field rhs){
    return lhs.name+lhs.value < rhs.name+rhs.value;
}

std::map<std::vector<field>,std::string>
rows_checksum(sql::Connection *con,const my::index& idx){
    std::string index_line=idx.get_full_index_names();

    std::string sql=boost::str(boost::format("SELECT %s , "
            "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#',%s)) AS UNSIGNED)), 10, 16)), 0) AS crc "
            "FROM `%s`.`%s` FORCE INDEX (`PRIMARY`) WHERE %s "
            "GROUP BY %s ORDER BY %s ;")
            % index_line % idx.get_col_names() % idx.get_dbname() % idx.get_tablename()
            % idx.get_sql_where() % index_line % index_line );

    if (DEBUG) { std::cerr << sql << std::endl; }
    auto stmt = con->createStatement();
    auto res = stmt->executeQuery(sql);
    std::map<std::vector<field>,std::string> ret;
    std::vector<field> tmp;
    for (const auto& v : idx.get_full_index()){
        tmp.push_back({v,""}); //Push fields name
    }
    while (res->next()) {
        for (auto& v : tmp){
            v.value = res->getString(v.name);
        }
        ret[tmp] = res->getString("crc");
    }
    delete res;
    delete stmt;
    return ret;
}

std::string vfield_to_str(const std::vector<field>& v,const std::string& spliter){
    std::string str;
    for (size_t i=0; i<v.size(); i++){
        str+="`"+v[i].name+"` = ";
        str+="'"+v[i].value+"'";
        if (i < v.size() - 1){
            str+=" "+spliter+" ";
        }
    }
    return str;
}

std::vector<field>
get_all_line(sql::Connection *con,const my::index& idx, const std::vector<field>& prikey ){
    std::string index_line=idx.get_full_index_names();

    std::string sql=boost::str(boost::format("SELECT %s FROM `%s`.`%s` FORCE INDEX (`PRIMARY`) WHERE %s LIMIT 1 ;")
            % idx.get_col_names() % idx.get_dbname() % idx.get_tablename() % vfield_to_str(prikey,"AND"));

    if (DEBUG) { std::cerr << sql << std::endl; }
    auto stmt = con->createStatement();
    auto res = stmt->executeQuery(sql);
    std::vector<field> ret;
    for (const auto& v : idx.get_colls() ){
        ret.push_back({v,""}); //Push fields name
    }
    while (res->next()) {
        for (auto& v : ret){
            v.value = res->getString(v.name);
        }
    }
    delete res;
    delete stmt;
    return ret;
}

void compare_tables(sql::Connection *con, sql::Connection *con2, my::index& irange){
        auto ch1 = checksum(con, irange);
        auto ch2 = checksum(con2, irange);
        if (DEBUG) {std::cerr << irange.get_start_stop() << std::endl;}
        if (ch1 != ch2){
            if (irange.index_can_split()) {
                std::pair<my::index,my::index> irp=irange.index_split(con,irange);
                compare_tables(con,con2,irp.first);
                compare_tables(con,con2,irp.second);
            }else{
                std::map<std::vector<field>,std::string> rows_chs = rows_checksum(con,irange);
                std::map<std::vector<field>,std::string> rows_chs2 = rows_checksum(con2,irange);
                {
                    std::vector<std::vector<field>> mark_for_remove;
                    for (auto r:rows_chs) {  // Delete doubles
                        if (rows_chs.count(r.first) + rows_chs2.count(r.first) == 2) {
                            if (rows_chs.at(r.first) == rows_chs2.at(r.first)) {
                                mark_for_remove.push_back(r.first);
                            }
                        }
                    }
                    for (const auto& v : mark_for_remove){
                        rows_chs.erase(v);
                        rows_chs2.erase(v);
                    }
                    mark_for_remove.clear();
                }
                for(auto r:rows_chs){
                    auto line=get_all_line(con,irange,r.first);
                    // REPLACE INTO `AppsgeyserStatistics`.`Report-date-is_tablet-action`(`date`, `is_tablet`, `action`, `count`)
                    // VALUES ('2018-04-19', '0', '1', '332364') ;
                    std::string name_line, val_line;
                    for (size_t i=0;i<line.size();i++){
                        name_line+="`"+line[i].name+"`";
                        val_line +="'"+line[i].value+"'";
                        if (i < line.size() -1 ){ // if not last line
                            name_line+=", ";
                            val_line+=", ";
                        }
                    }
                    std::string sql=boost::str(boost::format("REPLACE INTO `%s`.`%s`(%s) VALUES(%s) ;")
                            % irange.get_dbname() % irange.get_tablename() % name_line % val_line);

                    std::cout << sql << std::endl;
                    if (DEBUG) { std::cerr << "#Report size "<< rows_chs.size() << ":" << rows_chs2.size() << std::endl; }
                }

//                for(auto r:rows_chs2){ // Delete rows from server2
//                    auto line=r.first; //Use only primary index fields
//                    std::string where_line;
//                    for (size_t i=0; i<line.size(); i++){
//                        where_line+="`"+line[i].name+"`";
//                        where_line+="='"+line[i].value+"'";
//                        if (i < line.size() -1 ){ // if not last line
//                            where_line+=" AND ";
//                        }
//                    }
//                    std::string sql;
//                    sql+="DELETE FROM `"+tt2.dbname_+"`.`"+tt2.tablename_+"`";
//                    sql+=" WHERE "+where_line+" ; ";
//                    std::cout << sql << std::endl;
//                    rows_chs2.erase(r.first);
//                }
                if (DEBUG) { std::cerr << "#Report size "<< rows_chs.size() << ":" << rows_chs2.size() << std::endl; }

                if (DEBUG) {
                    std::cerr << "#Checksum not equal: " << irange.get_start_stop() << " "
                              << std::to_string(ch1.cnt) << ":" << ch1.crc << " "
                              << std::to_string(ch2.cnt) << ":" << ch2.crc << std::endl;
                }

            }
        }
    }


void compare_tables_iterator(sql::Connection *con, sql::Connection *con2, my::index& idx){

    my::index irange=idx.copy_index(); // Iteration by first index
    idx.index_iteration_first_step(irange);
    while(idx.index_iteration_can_next_step(irange)){
        compare_tables(con,con2,irange);
        idx.index_iteration_next_step(irange);
    }

}