//
// Created by Wei00161@umn.edu on 2020/5/14.
//

#include "zns_rocks/dm_zone.h"

#include <cassert>
#include <iostream>

#include "zns_rocks/zone_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

int main() {
  std::cout << "~~~~~~create DmZoneNamespace~~~~~~~~" << std::endl;
  std::shared_ptr<DmZoneNamespace> dmzonenamespace =
      DmZoneNamespace::CreatZoneNamespace();

  std::cout << "~~~~~~write~~~~~~~~" << std::endl;
  for (int i = 0; i < ZONEFile_NUMBER; ++i) {
    std::shared_ptr<Zone> dmzone = dmzonenamespace->GetZone(i);

    ZoneAddress zoneAddress;
    zoneAddress.zone_id = i;
    zoneAddress.offset = 0;
    std::string str = "that is test" + std::to_string(i);
    zoneAddress.length = str.size();
    dmzone->ZoneWrite(zoneAddress, str.c_str());
  }

  dmzonenamespace->CheckGC();

  std::cout << "~~~~~~~~~~~read~~~~~~~~~~~" << std::endl;
  for (int i = 0; i < ZONEFile_NUMBER; ++i) {
    std::shared_ptr<Zone> dmzone = dmzonenamespace->GetZone(i);

    ZoneAddress zoneAddress;
    zoneAddress.zone_id = i;
    zoneAddress.offset = 0;
    zoneAddress.length = dmzone->zoneInfo_.size;
    char result[dmzone->zoneInfo_.size];
    dmzone->ZoneRead(zoneAddress, result);
    std::cout << result << std::endl;
  }

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE
