//
// Created by Zhichao Cao czc199182@gmail.com 07/18/2020.
//

#include "zone_mapping.h"
#include "hm_zone.h"
#include "zns_file_writer.h"
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

    ZnsFileWriterManager* fw_manager = GetDefualtZnsFileWriterManager();
    ZoneMapping* zone_mapping = fw_manager->GetZoneMapping();
    Env* env = Env::Default();
    assert(zone_mapping != nullptr);

    WriteHints hints1;
    hints1.write_level = -1;
    hints1.file_cate = 2;
    ZnsFileWriter* fw1 = fw_manager->GetZnsFileWriter(hints1);
    assert(fw1 == nullptr);

    hints1.write_level = 0;
    ZnsFileWriter* fw2 = fw_manager->GetZnsFileWriter(hints1);
    assert(fw2 != nullptr);
    assert(fw2->GetUsageStatus() == false);
    fw2->SetInUse();
    assert(fw2->GetUsageStatus() == true);

    // the same writer is in use, so return null
    ZnsFileWriter* fw3 = fw_manager->GetZnsFileWriter(hints1);
    assert(fw3 == nullptr);

    ZnsZoneInfo* zfi_ptr = fw2->GetOpenZone();
    assert(zfi_ptr != nullptr);
    PrintZnsZoneInfo(zfi_ptr);
    fw_manager->CreateFileByThisWriter(env->NowMicros(), fw2, "test_file.sst", 1024);
    PrintZnsZoneInfo(zfi_ptr);
    ZnsFileInfo file_info;
    zone_mapping->GetZnsFileInfo("test_file.sst", &file_info);
    PrintZnsFileInfo(file_info);


    const char data[] = "we are write the new data of sst to this file";
    size_t len = sizeof(data);
    fw_manager->AppendDataOnFile("test_file.sst", len, data);
    PrintZnsZoneInfo(zfi_ptr);
    zone_mapping->GetZnsFileInfo("test_file.sst", &file_info);
    PrintZnsFileInfo(file_info);

    char buffer[18];
    fw_manager->ReadDataOnFile("test_file.sst", 0, 18, buffer);
    cout<<buffer<<endl;
    fw_manager->ReadDataOnFile("test_file.sst", 3, 18, buffer);
    cout<<buffer<<endl;
    char buffer1[18];
    fw_manager->ReadDataOnFile("test_file.sst", 40, 18, buffer1);
    cout<<buffer1<<endl;

    fw_manager->CloseFile(fw2, "test_file.sst");
    zone_mapping->GetZnsFileInfo("test_file.sst", &file_info);
    PrintZnsFileInfo(file_info);

    fw_manager->CreateFileByThisWriter(env->NowMicros(), fw2,
                "test_file1.sst", 512*1024*1024-30);
    zfi_ptr = fw2->GetOpenZone();
    PrintZnsZoneInfo(zfi_ptr);
    zone_mapping->GetZnsFileInfo("test_file1.sst", &file_info);
    PrintZnsFileInfo(file_info);

    fw_manager->DeleteFile(env->NowMicros(), "test_file.sst");
    PrintZnsZoneInfo(zfi_ptr);
    zone_mapping->GetZnsFileInfo("test_file.sst", &file_info);
    PrintZnsFileInfo(file_info);

    delete db;
    return 0;
}
