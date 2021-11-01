//
// Created by Zhichao Cao czc199182@gmail.com 07/28/2020.
//

#ifndef LEVELDB_ZNS_FILE_WRITER_H
#define LEVELDB_ZNS_FILE_WRITER_H

#include <unordered_map>
#include <map>
#include <vector>
#include <set>
#include <functional>

#include "leveldb/env.h"
#include "zone_test/zone_namespace.h"
#include "zone_test/zone_mapping.h"

namespace leveldb
{

struct WriteHints;

class ZnsFileWriter {
 public:
  ZnsFileWriter(ZnsZoneInfo* new_open_zone, int level, int score) {
    SetOpenZone(new_open_zone);
    is_in_use_ = false;
    level_ = level;
    score_ = score;
  }

  ~ZnsFileWriter() {}

  int GetZoneID() { return open_zone_->GetZoneID(); }

  Status SetOpenZone(ZnsZoneInfo* new_open_zone) {
    open_zone_ = new_open_zone;
    return Status::OK();
  }

  void SetInUse() { is_in_use_ = true; }

  void SetToAvailable() { is_in_use_ = false; }

  bool GetUsageStatus() { return is_in_use_; }

  void CreateFile(std::string file_name) {
    live_files_.insert(file_name);
    current_file_ = file_name;
  }

  void CloseFile(std::string file_name) {
    if (file_name == current_file_) {
      current_file_ = "";
    }
  }

  void RemoveFile(std::string file_name) {
    live_files_.erase(file_name);
    current_file_ = "";
  }

  Status RenameFile(const std::string& from, const std::string& to) {
    if (live_files_.find(from) == live_files_.end()) {
      return Status::NotFound(from);
    } else {
      live_files_.erase(from);
      live_files_.insert(to);
      return Status::OK();
    }
  }

  int GetScore() {
    return score_;
  }

  void SetScore(int score) {
    score_ = score;
  }

  ZnsZoneInfo* GetOpenZone() {
    return open_zone_;
  }

  std::string GetCurrentFileName() {
    return current_file_;
  }

 private:
  ZnsZoneInfo* open_zone_;
  std::set<std::string> live_files_;
  std::string current_file_;
  bool is_in_use_;
  int level_;
  int score_;
};

struct FileInfoInWriter {
  ZnsFileWriter* writer_ptr;
  uint64_t create_time;
  uint64_t delete_time;
};

class ZnsFileWriterManager {
 public:
  ZnsFileWriterManager(ZoneMapping* zone_mapping);

  ~ZnsFileWriterManager();

  //ZoneMapping::CreateFileOnZone is called inside this funciton
  // No need to call it again.
  // file_size_max is important, we need to estimate if current zone
  // available space is enough for this file. If not, we need to get another
  // empty zone for this writer.
  Status CreateFileByThisWriter(uint64_t now_time, ZnsFileWriter* writer,
              std::string file_name, size_t file_size_max);

 //ZoneMapping::DeleteFileOnZone is called inside this funciton
  Status DeleteFile(uint64_t now_time, std::string file_name);

  //ZoneMapping::RenameFileOnZone is called inside this function
  Status RenameFile(const std::string& from, const std::string& to);

  Status AppendDataOnFile(std::string file_name, size_t len, const char *buffer);

  Status ReadDataOnFile(std::string file_name, size_t offset,
          size_t len, const char *buffer);

  // when close is called, it will set this writer to available
  Status CloseFile(ZnsFileWriter* writer, std::string file_name);

  ZnsFileWriter* GetZnsFileWriter(WriteHints hints);

  Status AddFileWriter(int level, int score);

  ZoneMapping* GetZoneMapping() {
    return zone_mapping_;
  }

 private:
  ZoneMapping* zone_mapping_;
  std::unordered_map<std::string, FileInfoInWriter> file_to_writer_;
  std::unordered_map<int, std::map<int, ZnsFileWriter*, std::greater<int>>> file_writer_map_;
  std::vector<ZnsFileWriter*> file_writers_;

};

extern ZnsFileWriterManager* GetDefualtZnsFileWriterManager();

} // namespace leveldb
#endif
