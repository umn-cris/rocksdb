//
// Created by Zhichao Cao czc199182@gmail.com 07/18/2020.
//

#include "zone_mapping.h"
#include "hm_zone.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include <cassert>
#include <iostream>

using namespace std;
using namespace leveldb;

int main() {
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
    assert(status.ok());

    status = db->Put(WriteOptions(), "KeyNameExample", "ValueExample");
    assert(status.ok());
    string res;
    status = db->Get(ReadOptions(), "KeyNameExample", &res);
    assert(status.ok());
    cout << res << endl;


    shared_ptr<ZoneNamespace> zns_ptr = HmZoneNamespace::CreatZoneNamespace();
    ZoneMapping zone_mapping(zns_ptr, ZONEFile_NUMBER);
    Env* env = Env::Default();
    ZnsZoneInfo * zfi_ptr;
    Status s = zone_mapping.GetAndUseOneEmptyZone(&zfi_ptr);
    assert(s.ok());
    cout<<"zone info "<<zfi_ptr->zone_id<<" "<<zfi_ptr->valid_file_num<<" "<<zfi_ptr->valid_size<<"\n";
    size_t write_offset;
    zone_mapping.CreateFileOnZone(env->NowMicros(), "test_file.sst", zfi_ptr->zone_id, &write_offset);
    cout<<"zone info "<<zfi_ptr->zone_id<<" "<<zfi_ptr->valid_file_num<<" "<<zfi_ptr->valid_size<<"\n";
    ZnsFileInfo file_info;
    zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
    cout<<"File info "<<file_info.file_name<<" "<<file_info.create_time<<" "<<file_info.delete_time
        <<" "<<(int)file_info.f_stat<<" "<<file_info.length<<" "<<file_info.offset<<" "<<file_info.zone_id<<"\n";

    const char data[] = "we are write the new data of sst to this file";
    size_t len = sizeof(data);
    zone_mapping.WriteFileOnZone("test_file.sst", len, data);
    cout<<"zone info "<<zfi_ptr->zone_id<<" "<<zfi_ptr->valid_file_num<<" "<<zfi_ptr->valid_size<<"\n";
    zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
    cout<<"File info "<<file_info.file_name<<" "<<file_info.create_time<<" "<<file_info.delete_time
        <<" "<<(int)file_info.f_stat<<" "<<file_info.length<<" "<<file_info.offset<<" "<<file_info.zone_id<<"\n";

    char buffer[18];
    zone_mapping.ReadFileOnZone("test_file.sst", 0, 18, buffer);
    cout<<buffer<<endl;
    zone_mapping.ReadFileOnZone("test_file.sst", 3, 18, buffer);
    cout<<buffer<<endl;
    char buffer1[18];
    zone_mapping.ReadFileOnZone("test_file.sst", 40, 18, buffer1);
    cout<<buffer1<<endl;

    zone_mapping.CloseFileOnZone("test_file.sst");
    zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
    cout<<"File info "<<file_info.file_name<<" "<<file_info.create_time<<" "<<file_info.delete_time
        <<" "<<(int)file_info.f_stat<<" "<<file_info.length<<" "<<file_info.offset<<" "<<file_info.zone_id<<"\n";


    zone_mapping.DeleteFileOnZone(env->NowMicros(), "test_file.sst");
    cout<<"zone info "<<zfi_ptr->zone_id<<" "<<zfi_ptr->valid_file_num<<" "<<zfi_ptr->valid_size<<"\n";
    zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
    cout<<"File info "<<file_info.file_name<<" "<<file_info.create_time<<" "<<file_info.delete_time
        <<" "<<(int)file_info.f_stat<<" "<<file_info.length<<" "<<file_info.offset<<" "<<file_info.zone_id<<"\n";

    ZoneAddress zoneAddress;
    zoneAddress.zone_id = 0;
    zoneAddress.offset = 0;
    const char test[] = "that is a test";
    zoneAddress.length = sizeof(test);
    cout<<"~~~~~~write~~~~~~~~"<<endl;
    shared_ptr<HmZoneNamespace> hmzonenamespace = HmZoneNamespace::CreatZoneNamespace();
    shared_ptr<Zone> hmzone = hmzonenamespace->GetZone(0);
    hmzone->ZoneWrite(zoneAddress,test);

    cout<<"~~~~~~~~~~~read~~~~~~~~~~~"<<endl;
    char result[zoneAddress.length];
    hmzone->ZoneRead(zoneAddress,result);
    cout<<result<<endl;


    delete db;
    return 0;
}
