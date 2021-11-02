//
// Created by Zhichao Cao czc199182@gmail.com 07/18/2020.
//

#include "zone_mapping.h"

#include <cassert>
#include <iostream>

#include "hm_zone.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

int main() {
  DB* db;
  Options options;
  options.create_if_missing = true;
  Status status = DB::Open(options, "testdb", &db);
  assert(status.ok());

  status = db->Put(WriteOptions(), "KeyNameExample", "ValueExample");
  assert(status.ok());
  std::string res;
  status = db->Get(ReadOptions(), "KeyNameExample", &res);
  assert(status.ok());
  std::cout << res << std::endl;

  std::shared_ptr<ZoneNamespace> zns_ptr = HmZoneNamespace::CreatZoneNamespace();
  ZoneMapping zone_mapping(zns_ptr, ZONEFile_NUMBER);
  Env* env = Env::Default();
  ZnsZoneInfo* zfi_ptr;
  Status s = zone_mapping.GetAndUseOneEmptyZone(&zfi_ptr);
  assert(s.ok());
  std::cout << "zone info " << zfi_ptr->zone_id << " " << zfi_ptr->valid_file_num
       << " " << zfi_ptr->valid_size << "\n";
  size_t write_offset;
  zone_mapping.CreateFileOnZone(env->NowMicros(), "test_file.sst",
                                zfi_ptr->zone_id, &write_offset);
  std::cout << "zone info " << zfi_ptr->zone_id << " " << zfi_ptr->valid_file_num
       << " " << zfi_ptr->valid_size << "\n";
  ZnsFileInfo file_info;
  zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
  std::cout << "File info " << file_info.file_name << " " << file_info.create_time
       << " " << file_info.delete_time << " " << (int)file_info.f_stat << " "
       << file_info.length << " " << file_info.offset << " "
       << file_info.zone_id << "\n";

  const char data[] = "we are write the new data of sst to this file";
  size_t len = sizeof(data);
  zone_mapping.WriteFileOnZone("test_file.sst", len, data);
  std::cout << "zone info " << zfi_ptr->zone_id << " " << zfi_ptr->valid_file_num
       << " " << zfi_ptr->valid_size << "\n";
  zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
  std::cout << "File info " << file_info.file_name << " " << file_info.create_time
       << " " << file_info.delete_time << " " << (int)file_info.f_stat << " "
       << file_info.length << " " << file_info.offset << " "
       << file_info.zone_id << "\n";

  char buffer[18];
  zone_mapping.ReadFileOnZone("test_file.sst", 0, 18, buffer);
  std::cout << buffer << std::endl;
  zone_mapping.ReadFileOnZone("test_file.sst", 3, 18, buffer);
  std::cout << buffer << std::endl;
  char buffer1[18];
  zone_mapping.ReadFileOnZone("test_file.sst", 40, 18, buffer1);
  std::cout << buffer1 << std::endl;

  zone_mapping.CloseFileOnZone("test_file.sst");
  zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
  std::cout << "File info " << file_info.file_name << " " << file_info.create_time
       << " " << file_info.delete_time << " " << (int)file_info.f_stat << " "
       << file_info.length << " " << file_info.offset << " "
       << file_info.zone_id << "\n";

  zone_mapping.DeleteFileOnZone(env->NowMicros(), "test_file.sst");
  std::cout << "zone info " << zfi_ptr->zone_id << " " << zfi_ptr->valid_file_num
       << " " << zfi_ptr->valid_size << "\n";
  zone_mapping.GetZnsFileInfo("test_file.sst", &file_info);
  std::cout << "File info " << file_info.file_name << " " << file_info.create_time
       << " " << file_info.delete_time << " " << (int)file_info.f_stat << " "
       << file_info.length << " " << file_info.offset << " "
       << file_info.zone_id << "\n";

  ZoneAddress zoneAddress;
  zoneAddress.zone_id = 0;
  zoneAddress.offset = 0;
  const char test[] = "that is a test";
  zoneAddress.length = sizeof(test);
  std::cout << "~~~~~~write~~~~~~~~" << std::endl;
  std::shared_ptr<HmZoneNamespace> hmzonenamespace =
      HmZoneNamespace::CreatZoneNamespace();
  std::shared_ptr<Zone> hmzone = hmzonenamespace->GetZone(0);
  hmzone->ZoneWrite(zoneAddress, test);

  std::cout << "~~~~~~~~~~~read~~~~~~~~~~~" << std::endl;
  char result[zoneAddress.length];
  hmzone->ZoneRead(zoneAddress, result);
  std::cout << result << std::endl;

  delete db;
  return 0;
}

}  // namespace ROCKSDB_NAMESPACE
