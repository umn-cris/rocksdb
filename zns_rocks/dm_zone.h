//
// Created by Wei00161@umn.edu on 2020/5/14.
//

#pragma once

#include <fstream>
#include <list>
#include <unordered_map>
#include <vector>

#include "rocksdb/status.h"
#include "zone_namespace.h"

namespace ROCKSDB_NAMESPACE {

const long WEAR_LEVELING_WINDOW_SIZE = 5;

enum ZoneHotness { HOT, COLD };
static const std::string ZoneHotness[] = {"HOT", "COLD"};

class DmZone : public Zone {
 public:
  DmZone(){};
  ~DmZone(){};
  // need to assign: ZoneID, write_pointer, ....
  DmZone(size_t id);

  // based on Zone id to open file
  // set C++ put pointer according to the write_pointer in zone
  Status OpenZone() override;

  Status CloseZone() override;

  Status FinishZone() override;

  ZoneInfo ReportZone() override;

  // should not call it via shared ptr, should use zns to call
  Status ResetWritePointer() override;

  Status ZoneRead(ZoneAddress addr, char* data) override;

  Status ZoneWrite(ZoneAddress addr, const char* data) override;

  std::string ToString() const {
    std::string str = std::to_string(zoneInfo_.id);

    str += ";";
    str += std::to_string(zoneInfo_.write_pointer);

    str += ";";
    str += TypeStr[zoneInfo_.zone_type];

    str += ";";
    str += ConditionStr[zoneInfo_.zone_condition];

    str += ";";
    str += StateStr[zoneInfo_.zone_state];
    return str;
  }

  size_t GetZoneId() { return zoneInfo_.id; }

 private:
  // fstream& modify_zone_;
  enum ZoneHotness zone_hotness_ = COLD;
};

class DmZoneNamespace : public ZoneNamespace {
 public:
  DmZoneNamespace();
  ~DmZoneNamespace() = default;
  static std::shared_ptr<DmZoneNamespace> CreatZoneNamespace() {
    return std::shared_ptr<DmZoneNamespace>(new DmZoneNamespace());
  }
  size_t GetZoneCount() override { return zones_.size(); }

  // Create a new zone.
  Status NewZone() override;

  // input: logical zone id, return mapped physical zone ptr
  std::shared_ptr<Zone> GetZone(int id) override {
    Status status;
    std::shared_ptr<Zone> res_zone;
    int pla;

    auto it_LBA_2_PLA = LBA_2_PBA_.find(id);
    if (it_LBA_2_PLA != LBA_2_PBA_.end()) {
      pla = LBA_2_PBA_[id];
      // status = Status::OK();
    } else {
      status = Status::NotFound(
          "[Zone_namespace.h] [RemoveZone] no zone to get, zone id: " +
          std::to_string(id));
    }

    auto it_PLA = zones_.find(pla);
    if (it_PLA != zones_.end()) {
      res_zone = it_PLA->second;
      // status = Status::OK();
    } else {
      status = Status::NotFound(
          "[Zone_namespace.h] [RemoveZone] no zone to get, zone id: " +
          std::to_string(id));
    }
    return res_zone;
  }

  Status Resetptr(int id) override;

  Status RemoveZone(int id) override {
    Status status;
    auto it = zones_.find(id);
    if (it != zones_.end()) {
      zones_.erase(it);
      status = Status::OK();
    } else {
      status = Status::NotFound(
          "[Zone_namespace.h] [RemoveZone] no zone to remove, zone id: " +
          std::to_string(id));
    }
    return status;
  }

  Status InitZNS(const char* dir_name) override;
  Status InitZone(const char* Zonepath, const char* filename,
                  char* filepath) override;
  // Status Write(ZoneAddress addr, const char* data) override;
  // Status Read(ZoneAddress addr,  char* data) override;
  // swap data in hot zone and cold zone, triggered when hot zone is out of
  // window. data in the hot zone will be migrate back to the lowest cold zone
  Status SwapZone(std::list<std::shared_ptr<DmZone>>::iterator,
                  std::list<std::shared_ptr<DmZone>>::iterator);
  // when hot zone is out of window, call GC
  Status GC(std::list<std::shared_ptr<DmZone>>::iterator iter);
  Status CheckGC();
  void CheckWindow() {
    int i = 0;
    for (auto n : window_) {
      std::cout << "level " << i++ << ": ";
      for (auto m : n) {
        std::cout << m->zoneInfo_.id << " ";
      }
      std::cout << std::endl;
    }
  }

 private:
  int next_zone_id_ = 0;
  // the zone_id in zones_ is PBA
  std::unordered_map<int, std::shared_ptr<DmZone>> zones_;
  // need a mapping between LBA to PBA
  // first int is Logical zone id while second int is physical zone id
  // this mapping only used in DM & HA ZNS, for HM ZNS, all GC and migration
  // will be done by host
  std::unordered_map<int, int> LBA_2_PBA_;
  // wear-leveling used window. Zone with same erase count will insert in same
  // list. list contain the shared-ptr of those zone.
  std::vector<std::list<std::shared_ptr<DmZone>>> window_;

  // min-wear / max-wear denote the minimum/maximum erase count of the zones in
  // above lists
  size_t min_wear_ = 0;
  size_t max_wear_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
