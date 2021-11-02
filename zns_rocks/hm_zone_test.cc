//
// Created by Wei00161@umn.edu on 2020/5/15.
//

//
// Created by Wei00161@umn.edu on 2020/5/14.
//

#include "zns_rocks/hm_zone.h"
#include <cassert>
#include <iostream>
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
