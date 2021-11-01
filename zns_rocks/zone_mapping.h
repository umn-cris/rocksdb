//
// Created by Zhichao Cao czc199182@gmail.com 07/18/2020.
//

#ifndef LEVELDB_ZONE_MAPPING_H
#define LEVELDB_ZONE_MAPPING_H

#include <map>
#include <vector>
#include <unordered_map>

#include "leveldb/env.h"
#include "zone_test/zone_namespace.h"

namespace leveldb
{

  enum class ZnsFileStat
  {
    kCreated,
    kWrited,
    kClosed,
    kDeleted,
  };

  struct ZnsFileInfo
  {
    std::string file_name;
    int zone_id;
    size_t offset;
    size_t length;
    ZnsFileStat f_stat;
    uint64_t create_time;
    uint64_t delete_time;
  };

  struct ZnsZoneInfo
  {
    int zone_id;
    std::shared_ptr<Zone> zone_ptr;
    size_t valid_size;
    int valid_file_num;
    std::unordered_map<std::string, ZnsFileInfo *> files_map;

    size_t GetWritePointerOffset() {
      ZoneInfo z_info = zone_ptr->ReportZone();
      return z_info.write_pointer;
    }

    int GetZoneID() {return zone_id;}
  };

  class ZoneMapping
  {
  public:
    ZoneMapping(std::shared_ptr<ZoneNamespace> zns, int zone_num);
    ~ZoneMapping() {};
    // must be called before this zone is used for write
    // called by the log writer to hold this zone for write
    //
    // The wear-leveling policy should be used here; So
    // the interface will be changed here later;
    Status GetAndUseOneEmptyZone(ZnsZoneInfo **z_info_ptr);

    // the following APIs will be directly called in ZnsEnv
    // Log writer is the control (with lock) of write permission
    // and where to write.
    Status CreateFileOnZone(uint64_t now_time, std::string file_name, int zone_id, size_t* offset);

    Status DeleteFileOnZone(uint64_t now_time, std::string file_name);

    Status RenameFileOnZone(const std::string& from, const std::string& to);

    Status CloseFileOnZone(std::string file_name);

    Status ReadFileOnZone(std::string file_name, size_t offset, size_t len, const char *buffer);

    Status WriteFileOnZone(std::string file_name, size_t len, const char *buffer);

    Status GetZnsFileInfo(std::string file_name, ZnsFileInfo* file_ptr);

    // This function suggested to be called before other file related are called
    bool IsFileInZone(std::string file_name);

  private:
    std::unordered_map<std::string, ZnsFileInfo> files_map_;
    std::unordered_map<int, ZnsZoneInfo *> used_zones_;
    std::unordered_map<int, ZnsZoneInfo *> empty_zones_;
    std::vector<ZnsZoneInfo> zone_list_;
    std::shared_ptr<ZoneNamespace> zns_ptr_;
    int zone_num_;
  };

extern ZoneMapping* GetDefaultZoneMapping();

extern void PrintZnsZoneInfo(ZnsZoneInfo* zfi_ptr);

extern void PrintZnsFileInfo(ZnsFileInfo& file_info);

} // namespace leveldb
#endif
