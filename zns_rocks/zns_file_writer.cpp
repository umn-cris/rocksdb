//
// Created by Zhichao Cao czc199182@gmail.com 07/28/2020.
//

#include "zns_file_writer.h"

namespace leveldb {

  ZnsFileWriterManager::ZnsFileWriterManager(ZoneMapping* zone_mapping) {
    zone_mapping_ = zone_mapping;
    int logger_num = 3;
    for (int i=0; i< logger_num; i++) {
      Status s = AddFileWriter(i, INT_MAX);
      assert(s.ok());
    }
  }

  ZnsFileWriterManager::~ZnsFileWriterManager() {
    for (int i = 0; i < file_writers_.size(); i++) {
      if (file_writers_[i] != nullptr) {
        free(file_writers_[i]);
      }
    }
  }

  Status ZnsFileWriterManager::CreateFileByThisWriter(uint64_t now_time,
      ZnsFileWriter* writer, std::string file_name, size_t file_size_max) {
    if (writer == nullptr) {
      return Status::Corruption("no file writer");
    }
    auto zone = writer->GetOpenZone();
    auto zone_info = zone->zone_ptr->ReportZone();
    size_t available_space = zone_info.size - zone_info.write_pointer;
    if (available_space <= file_size_max + 1024) {
        // zone is not enough for this file, switch to an empty zone
      ZnsZoneInfo* z_info;
      Status s = zone_mapping_->GetAndUseOneEmptyZone(&z_info);
      if (!s.ok() || z_info == nullptr) {
        return Status::NotFound("not find empty zone");
      }
      writer->SetOpenZone(z_info);
    }

    auto found = file_to_writer_.find(file_name);
    if (found != file_to_writer_.end()) {
      found->second.create_time = now_time;
      found->second.delete_time = 0;
      found->second.writer_ptr->RemoveFile(file_name);

      found->second.writer_ptr = writer;
      writer->CreateFile(file_name);
      size_t write_offset;
      return zone_mapping_->CreateFileOnZone(now_time, file_name, writer->GetZoneID(), &write_offset);
    }
    FileInfoInWriter info;
    info.create_time = now_time;
    info.delete_time = 0;
    info.writer_ptr = writer;
    file_to_writer_.insert(std::make_pair(file_name, info));
    writer->CreateFile(file_name);
    size_t write_offset;
    return zone_mapping_->CreateFileOnZone(now_time, file_name,writer->GetZoneID(), &write_offset);
  }

  Status ZnsFileWriterManager::DeleteFile(uint64_t now_time, std::string file_name) {
    auto found = file_to_writer_.find(file_name);
    if (found == file_to_writer_.end()) {
      return zone_mapping_->DeleteFileOnZone(now_time, file_name);
    }
    found->second.delete_time = now_time;
    auto ptr = found->second.writer_ptr;
    ptr->RemoveFile(file_name);
    return zone_mapping_->DeleteFileOnZone(now_time, file_name);
  }

  Status ZnsFileWriterManager::RenameFile(const std::string& from, const std::string& to) {
    auto found = file_to_writer_.find(from);
    if (found == file_to_writer_.end()) {
      return zone_mapping_->RenameFileOnZone(from, to);
    }
    auto ptr = found->second.writer_ptr;
    Status s = ptr->RenameFile(from, to);
    if (!s.ok()) {
      return s;
    }
    return zone_mapping_->RenameFileOnZone(from, to);
  }

  Status ZnsFileWriterManager::AppendDataOnFile(std::string file_name, size_t len, const char *buffer) {
    auto found = file_to_writer_.find(file_name);
    if (found == file_to_writer_.end()) {
      return Status::Corruption("File Does not exit!");
    }
    auto writer = found->second.writer_ptr;
    if (file_name != writer->GetCurrentFileName()) {
      return Status::Corruption("Writer does not hold this file");
    }
    return zone_mapping_->WriteFileOnZone(file_name, len, buffer);
  }

  Status ZnsFileWriterManager::ReadDataOnFile(std::string file_name, size_t offset,
          size_t len, const char *buffer) {
    auto found = file_to_writer_.find(file_name);
    if (found == file_to_writer_.end()) {
      return Status::Corruption("File Does not exit!");
    }
    return zone_mapping_->ReadFileOnZone(file_name, offset, len, buffer);
  }


  Status ZnsFileWriterManager::CloseFile(ZnsFileWriter* writer, std::string file_name) {
    if(writer == nullptr) {
      return Status::Corruption("writer is null!");
    }
    writer->CloseFile(file_name);
    writer->SetToAvailable();
    return zone_mapping_->CloseFileOnZone(file_name);
  }


  ZnsFileWriter* ZnsFileWriterManager::GetZnsFileWriter(WriteHints hints) {
    if (hints.write_level < 0) {
      return nullptr;
    }
    if(file_writer_map_.find(hints.write_level) == file_writer_map_.end()) {
      Status s = AddFileWriter(hints.write_level, INT_MAX);
      if (!s.ok()) {
        return nullptr;
      }
    }
    auto sub_writers = file_writer_map_[hints.write_level];
    int score = INT_MAX;   // to be replaced by the real score
    ZnsFileWriter* ret = nullptr;
    for (auto i = sub_writers.begin(); i != sub_writers.end(); i++) {
      if (score >= i->second->GetScore()) {
        ret = i->second;
        break;
      }
    }
    if (ret == nullptr) {
      ret = sub_writers.begin()->second;
    }
    if (ret->GetUsageStatus()) {
      return nullptr;
    }
    return ret;
  }

  Status ZnsFileWriterManager::AddFileWriter(int level, int score) {
    ZnsZoneInfo* z_info;
    zone_mapping_->GetAndUseOneEmptyZone(&z_info);
    if (z_info == nullptr) {
      return Status::NotFound("not find empty zone");
    }
    ZnsFileWriter* file_writer = new ZnsFileWriter(z_info, level, score);
    file_writers_.push_back(file_writer);
    file_writer_map_[level][score] = file_writers_.back();
    return Status::OK();
  }

ZnsFileWriterManager* GetDefualtZnsFileWriterManager() {
  static ZnsFileWriterManager* zfm_ptr = new ZnsFileWriterManager(GetDefaultZoneMapping());
  return zfm_ptr;
}


} //name space
