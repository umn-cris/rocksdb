//
// Created by Zhichao Cao czc199182@gmail.com 07/18/2020.
//

#include "zone_mapping.h"
#include "hm_zone.h"

namespace leveldb {

ZoneMapping::ZoneMapping(std::shared_ptr<ZoneNamespace> zns, int zone_num) {
  zns_ptr_ = zns;
  zone_num_ = zone_num;

  // Initilize the zone info list
  for(int i = 0; i < zone_num_; i++) {
    ZnsZoneInfo z_info;
    std::unordered_map<std::string, ZnsFileInfo*> tmp_map;
    z_info.zone_id = i;
    z_info.zone_ptr = zns_ptr_->GetZone(i);
    z_info.valid_size = 0;
    z_info.files_map = tmp_map;
    z_info.valid_file_num = 0;
    zone_list_.push_back(z_info);
  }
  // store the zone pointers in the empty_zones_
  for(int i = 0; i < zone_num_; i++) {
    empty_zones_.insert(std::make_pair(i, &zone_list_[i]));
  }
}

Status ZoneMapping::GetAndUseOneEmptyZone(ZnsZoneInfo** z_info_ptr) {
  if (empty_zones_.size() == 0) {
    return Status::NotFound("invalid");
  }

  auto tmp = empty_zones_.begin();
  if (tmp == empty_zones_.end()) {
    return Status::NotFound("invalid");
  }
  *z_info_ptr = tmp->second;
  empty_zones_.erase(tmp->second->zone_id);
  used_zones_.insert(std::make_pair(tmp->second->zone_id, tmp->second));
  return Status::OK();
}

Status ZoneMapping::CreateFileOnZone(uint64_t now_time, std::string file_name, int zone_id, size_t* offset) {
  auto found = files_map_.find(file_name);
  auto z = used_zones_.find(zone_id);
  if (found != files_map_.end() || z == used_zones_.end()) {
    return Status::InvalidArgument("invalid");
  }

  // Increase the valid file number
  z->second->valid_file_num += 1;
  auto zone_ptr = z->second->zone_ptr;
  ZoneInfo z_info = zone_ptr->ReportZone();
  ZnsFileInfo tmp_info;
  tmp_info.file_name = file_name;
  tmp_info.zone_id = z_info.id;
  tmp_info.offset = z_info.write_pointer;
  tmp_info.length = 0;
  tmp_info.f_stat = ZnsFileStat::kCreated;
  tmp_info.create_time = now_time;
  tmp_info.delete_time = 0;
  files_map_.insert(std::make_pair(file_name, tmp_info));
  z->second->files_map.insert(std::make_pair(file_name, &(files_map_[file_name])));
  *offset = tmp_info.offset;
  return Status::OK();
}

Status ZoneMapping::DeleteFileOnZone(uint64_t now_time, std::string file_name) {
  auto found = files_map_.find(file_name);
  if (found == files_map_.end()) {
    return Status::OK();
  }
  auto z = used_zones_.find(found->second.zone_id);
  if (z == used_zones_.end()) {
    return Status::Corruption("invalid");
  }

  // update the zone info;
  z->second->valid_size -= found->second.length;
  z->second->valid_file_num -=1;
  z->second->files_map.erase(file_name);
  auto z_info = z->second;
  if (z_info->valid_size == 0) {
    z_info->zone_ptr->ResetWritePointer();
    empty_zones_.insert(std::make_pair(z_info->zone_id, z_info));
    used_zones_.erase(z_info->zone_id);
  }

  // Update the file info
  found->second.f_stat = ZnsFileStat::kDeleted;
  found->second.delete_time = now_time;
  found->second.length = 0;
  found->second.offset = 0;
  found->second.zone_id = -1;
  return Status::OK();
}

Status ZoneMapping::RenameFileOnZone(const std::string& from, const std::string& to) {
  auto found = files_map_.find(from);
  if (found == files_map_.end()) {
    return Status::InvalidArgument("invalid");
  }

  found->second.file_name = to;
  return Status::OK();
}

Status ZoneMapping::CloseFileOnZone(std::string file_name) {
  auto found = files_map_.find(file_name);
  if (found == files_map_.end()) {
    return Status::InvalidArgument("invalid");
  }
  if (found->second.f_stat == ZnsFileStat::kDeleted) {
    return Status::InvalidArgument("invalid");
  }
  auto z = used_zones_.find(found->second.zone_id);
  if (z == used_zones_.end()) {
    return Status::Corruption("invalid");
  }
  found->second.f_stat = ZnsFileStat::kClosed;
  return Status::OK();
}

Status ZoneMapping::ReadFileOnZone(std::string file_name, size_t offset,
                                    size_t len, const char *buffer) {
  auto found = files_map_.find(file_name);
  if (found == files_map_.end()) {
    return Status::InvalidArgument("invalid");
  }
  if (found->second.f_stat == ZnsFileStat::kDeleted) {
    return Status::InvalidArgument("invalid");
  }
  auto z = used_zones_.find(found->second.zone_id);
  if (z == used_zones_.end()) {
    return Status::Corruption("invalid");
  }

 if (offset > found->second.length) {
    return Status::InvalidArgument("invalid");
  }

  size_t valid_len;
  if (offset+len > found->second.length) {
    valid_len = found->second.length - offset;
  } else {
    valid_len = len;
  }

  ZoneAddress z_address;
  z_address.zone_id = z->second->zone_id;
  z_address.offset = found->second.offset + offset;
  z_address.length = valid_len;
  char *data = (char*)buffer;
  return z->second->zone_ptr->ZoneRead(z_address, data);
}

Status ZoneMapping::WriteFileOnZone(std::string file_name,
                                  size_t len, const char *buffer) {
  auto found = files_map_.find(file_name);
  if (found == files_map_.end()) {
    return Status::InvalidArgument("invalid");
  }
  if (found->second.f_stat == ZnsFileStat::kDeleted) {
    return Status::InvalidArgument("invalid");
  }
  auto z = used_zones_.find(found->second.zone_id);
  if (z == used_zones_.end()) {
    return Status::Corruption("invalid");
  }

  ZoneInfo z_info = z->second->zone_ptr->ReportZone();

  ZoneAddress z_address;
  z_address.zone_id = z->second->zone_id;
  z_address.offset = z_info.write_pointer;
  z_address.length = len;
  Status s = z->second->zone_ptr->ZoneWrite(z_address, buffer);
  if (!s.ok()) {
    return s;
  }

  // Updates the metadata
  found->second.length += len;
  z->second->valid_size += len;
  return Status::OK();
}

Status ZoneMapping::GetZnsFileInfo(std::string file_name, ZnsFileInfo* file_ptr) {
  auto found = files_map_.find(file_name);
  if (found == files_map_.end()) {
    return Status::NotFound("Not find");
  }
  *file_ptr = found->second;
  return Status::OK();
}

bool ZoneMapping::IsFileInZone(std::string file_name) {
  auto found = files_map_.find(file_name);
  if (found == files_map_.end()) {
    return false;
  }

  if(found->second.f_stat == ZnsFileStat::kDeleted) {
    return false;
  }
  return true;
}

ZoneMapping* GetDefaultZoneMapping() {
  std::shared_ptr<ZoneNamespace> zns_ptr = HmZoneNamespace::CreatZoneNamespace();
  static ZoneMapping* zm_ptr = new ZoneMapping(zns_ptr, ZONEFile_NUMBER);
  return zm_ptr;
}

void PrintZnsZoneInfo(ZnsZoneInfo* zfi_ptr) {
  if (zfi_ptr == nullptr) {
    cout<<"zns_zone_info is nullptr!\n";
  }
  cout<<"zone id: "<<zfi_ptr->zone_id<<" valid_file_num: "
      <<zfi_ptr->valid_file_num<<" valid_size: "<<zfi_ptr->valid_size<<"\n";
}

void PrintZnsFileInfo(ZnsFileInfo& file_info) {
  cout<<"File name: "<<file_info.file_name<<" create_time: "<<file_info.create_time
      <<" delete_time: "<<file_info.delete_time<<" file_status: "
      <<(int)file_info.f_stat<<" file_length: "<<file_info.length<<" file_offset_in_zone: "
      <<file_info.offset<<" zone_id: "<<file_info.zone_id<<"\n";
}

} //name space
