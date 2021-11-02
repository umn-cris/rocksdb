//
// Created by Wei00161@umn.edu on 2020/5/16.
//

#include "zns_rocks/dm_zone.h"

#include <dirent.h>
#include <sys/stat.h>

#include <cassert>
#include <iostream>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
static std::string path = "Dm_zones_";

DmZone::DmZone(std::fstream &fs, size_t id) {
  // zone_info_ = (struct ZoneInfo*)malloc(sizeof(struct ZoneInfo));
  zoneInfo_.id = id;
  zoneInfo_.write_pointer = 0;
  zoneInfo_.zone_type = SEQUENTIAL_WRITE_REQUIRED;
  zoneInfo_.zone_condition = CLOSED;
  zoneInfo_.zone_state = SEQUENTIAL;
  std::fstream modify_zone_;
  modify_zone_.open(path + "/" + std::to_string(zoneInfo_.id), std::ios::out);
  if (!modify_zone_.is_open())
    std::cout << "[Dm_zone.cpp] [DmZone] create zone file failed" << std::endl;
  // modify_zone_<<ToString()<<endl;
  modify_zone_.close();
  if (if_debug) std::cout << "create zone " << zoneInfo_.id << std::endl;
}

// for correctness, OpenZone only change zone state. Zone file will only be open
// when data will be written or read from zone file
Status DmZone::OpenZone() {
  Status status;
  /*modify_zone_.open(path+"/"+to_string(zoneInfo_.id), ios::out|ios::in);
  if(!modify_zone_.is_open()){
      status = Status::NotFound("[Dm_zone.cpp] [OpenZone] Open zone failed, zone
  id: "+ to_string(zoneInfo_.id)); return status;
  }*/
  zoneInfo_.zone_condition = OPEN;
  status = Status::OK();
  if (if_debug) std::cout << "open zone " << zoneInfo_.id << std::endl;
  return status;
}

Status DmZone::FinishZone() {
  Status status;
  zoneInfo_.write_pointer = ZONESIZE;
  zoneInfo_.size = ZONESIZE;
  status = Status::OK();
  if (if_debug) std::cout << "finishe zone " << zoneInfo_.id << std::endl;
  return status;
}

ZoneInfo DmZone::ReportZone() {
  if (if_debug) std::cout << "report zone " << zoneInfo_.id << std::endl;
  return zoneInfo_;
}

// should not call it via shared ptr, should use zns to call
Status DmZone::ResetWritePointer() {
  Status status;
  zoneInfo_.write_pointer = 0;
  zoneInfo_.size = 0;
  zoneInfo_.erase_count_++;
  // modify_zone_.seekp(0,ios::beg);
  // modify_zone_<<ToString()<<endl;
  // no close here!
  status = Status::OK();
  if (if_debug)
    std::cout << "in [dm_zone] [ResetWritePointer] reset zone [" << zoneInfo_.id
              << "]" << std::endl;

  return status;
}

// same reason with OpenZone, when finish writing or read  data from zone file,
// it will be closed rather than here
Status DmZone::CloseZone() {
  Status status;
  zoneInfo_.zone_condition = CLOSED;
  // modify_zone_.seekp(0,ios::beg);
  // modify_zone_<<ToString()<<endl;
  // modify_zone_.close();
  status = Status::OK();
  if (if_debug) std::cout << "close zone " << zoneInfo_.id << std::endl;
  return status;
}

Status DmZone::ZoneWrite(ZoneAddress addr, const char *data) {
  Status status;
  // to write data in zone file, first open it
  // only change zone state, open operation will be handle manually in next line
  // this will help avoid unexpected bug (i.e., avoid opening a file both for
  // read and write, and reset read/write pointer at the same time)
  std::cout << "ready to write zone [" << addr.zone_id << "]";
  status = OpenZone();
  if (zoneInfo_.write_pointer + addr.length > ZONESIZE) {
    status = Status::InvalidArgument(
        "[Dm_zone.cpp] [ZoneWrite] WriteZone failed, reach end of zone file, "
        "zone id: " +
        std::to_string(zoneInfo_.id));
    return status;
  }
  // only "ios::out" rather than "ios::out|ios::in"
  std::fstream modify_zone_;
  modify_zone_.open(path + "/" + std::to_string(zoneInfo_.id),
                    std::ios::out | std::ios::binary);
  if (!modify_zone_.is_open()) {
    status = Status::NotFound(
        "[Dm_zone.cpp] [ZoneWrite] OpenZone failed, zone id: " +
        std::to_string(zoneInfo_.id));
    return status;
  }

  // if comes a non-sequential write request
  if (addr.offset != zoneInfo_.write_pointer) {
    // for a sequential required zone,
    if (zoneInfo_.zone_type == SEQUENTIAL_WRITE_REQUIRED) {
      std::cout << "" << std::endl;
      status.NotSupported(
          "[Dm_zone.cpp] [ZoneWrite] it is not a sequential write, sequential "
          "write required. Zone id: " +
          std::to_string(addr.zone_id));
      return status;
    } else if (zoneInfo_.zone_type == SEQUENTIAL_WRITE_PREFERRED) {
      zoneInfo_.zone_state = NON_SEQUENTIAL;
    }
    zoneInfo_.write_pointer = addr.offset;
  }
  modify_zone_.seekp(addr.offset);
  modify_zone_.write(data, addr.length);
  if (!modify_zone_.good()) {
    if (modify_zone_.fail())
      status = Status::InvalidArgument(
          "[Dm_zone.cpp] [ZoneWrite] WriteZone failed, no remain space / write "
          "mode error / format error... zone id: " +
          std::to_string(zoneInfo_.id));
    else
      status = Status::InvalidArgument(
          "[Dm_zone.cpp] [ZoneWrite] WriteZone failed, unknown reasons, need "
          "further checking. zone id: " +
          std::to_string(zoneInfo_.id));
  }
  modify_zone_.close();
  zoneInfo_.size += addr.length;
  zoneInfo_.write_pointer += addr.length;
  return status;
}

Status DmZone::ZoneRead(ZoneAddress addr, char *data) {
  Status status;
  std::cout << "ready to read zone [" << addr.zone_id << "]";
  status = OpenZone();
  std::fstream modify_zone_;
  modify_zone_.open(path + "/" + std::to_string(zoneInfo_.id),
                    std::ios::in | std::ios::binary);

  if (!modify_zone_.is_open()) {
    status =
        Status::NotFound("[Dm_zone.cpp] [ZoneRead] OpenZone failed, zone id: " +
                         std::to_string(zoneInfo_.id));
    return status;
  }

  modify_zone_.seekg(addr.offset);
  modify_zone_.read(data, addr.length);
  if (!modify_zone_.good()) {
    if (modify_zone_.eof())
      status = Status::InvalidArgument(
          "[Dm_zone.cpp] [ZoneRead] ReadZone failed, reach end of zone file, "
          "zone id: " +
          std::to_string(zoneInfo_.id));
    else if (modify_zone_.fail())
      status = Status::InvalidArgument(
          "[Dm_zone.cpp] [ZoneRead] ReadZone failed, no remain space / read "
          "mode error / format error... zone id: " +
          std::to_string(zoneInfo_.id));
  }
  modify_zone_.close();
  return status;
}

/*Status DmZoneNamespace::Write(ZoneAddress addr, const char *data) {
    Status status;
    fstream fs;
    DmZone write_zone(fs);

    // get target zone
    status = GetZone(addr.zone_id,write_zone);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[Dm_zone.cpp] [Write] in ZNS write, GetZone
failed, zone id: " + to_string(addr.zone_id)); return status;
    }

    // call target zone's zone write function
    status = write_zone.ZoneWrite(addr,data);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[Dm_zone.cpp] [Write] in ZNS write, WriteZone
failed, zone id: " + to_string(addr.zone_id)); return status;
    }
}*/

/*Status DmZoneNamespace::Read(ZoneAddress addr,  char *data) {
    Status status;
    fstream fs;
    DmZone read_zone(fs);

    // get target zone
    status = GetZone(addr.zone_id,read_zone);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[Dm_zone.cpp] [Read] in ZNS read, GetZone
failed, zone id: " + to_string(addr.zone_id)); return status;
    }

    // call target zone's zone write function
    status = read_zone.ZoneRead(addr,data);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[Dm_zone.cpp] [Read] in ZNS read, ReadZone
failed, zone id: " + to_string(addr.zone_id)); return status;
    }
    return status;
}*/

Status DmZoneNamespace::NewZone() {
  Status status;
  std::fstream fs;
  // begin: modification for shared_ptr
  std::shared_ptr<DmZone> zone_ptr(new DmZone(fs, next_zone_id_));
  auto it = zones_.emplace(next_zone_id_, zone_ptr);
  // end
  if (!it.second) {
    status = Status::InvalidArgument(
        "[Dm_zone.cpp] [NewZone] already has a zone in ZNS, zone id: " +
        std::to_string(next_zone_id_));
    return status;
  }
  // all new zone should insert in the wear-leveling window. since they are new
  // zone, insert in the 1st list.
  window_[0].push_back(zone_ptr);
  // for new create zone, its LBA = its PBA
  LBA_2_PBA_.emplace(next_zone_id_, next_zone_id_);

  ++next_zone_id_;
  status = Status::OK();
  return status;
}

Status DmZoneNamespace::SwapZone(
    std::list<std::shared_ptr<DmZone>>::iterator bottom_zone,
    std::list<std::shared_ptr<DmZone>>::iterator top_zone) {
  Status status;
  // lock
  // migrate data, need to read from both bottom zone & top zone, and write into
  // 2 zones
  // read process
  ZoneAddress bottomzone_Address;
  bottomzone_Address.zone_id = (*bottom_zone)->zoneInfo_.id;
  bottomzone_Address.offset = 0;
  bottomzone_Address.length = (*bottom_zone)->zoneInfo_.size;
  char bottom_result[(*bottom_zone)->zoneInfo_.size];
  status = (*bottom_zone)->ZoneRead(bottomzone_Address, bottom_result);
  if (!status.ok()) {
    status = Status::NotFound(
        "[dm_zone.cpp] [SwapZone] top zone's write ptr > 0, fail to read from "
        "bottom zone ");
    return status;
  }

  ZoneAddress topzone_Address;
  topzone_Address.zone_id = (*top_zone)->zoneInfo_.id;
  topzone_Address.offset = 0;
  topzone_Address.length = (*top_zone)->zoneInfo_.size;
  char top_result[(*top_zone)->zoneInfo_.size];
  status = (*top_zone)->ZoneRead(topzone_Address, top_result);
  if (!status.ok()) {
    status = Status::NotFound(
        "[dm_zone.cpp] [SwapZone] top zone's write ptr > 0, fail to read from "
        "top zone ");
    return status;
  }
  //  after read, reset then write
  (*bottom_zone)->ResetWritePointer();
  (*top_zone)->ResetWritePointer();

  // write process
  const char *migrate_data = bottom_result;
  status = (*top_zone)->ZoneWrite(topzone_Address, migrate_data);
  if (!status.ok()) {
    status = Status::NotFound(
        "[dm_zone.cpp] [SwapZone] top zone's write ptr is 0, fail to write to "
        "top zone ");
    return status;
  }
  migrate_data = top_result;
  status = (*bottom_zone)->ZoneWrite(bottomzone_Address, migrate_data);
  if (!status.ok()) {
    status = Status::NotFound(
        "[dm_zone.cpp] [SwapZone] top zone's write ptr is 0, fail to write to "
        "bottom zone ");
    return status;
  }

  // swap LBA2PBA mapping
  bool find_bottomzone = false;
  bool find_topzone = false;
  auto it = LBA_2_PBA_.begin();
  while (it != LBA_2_PBA_.end()) {
    if (it->second == (*bottom_zone)->zoneInfo_.id) {
      it->second = (*top_zone)->zoneInfo_.id;
      find_bottomzone = true;
    }

    if (it->second == (*top_zone)->zoneInfo_.id) {
      it->second = (*bottom_zone)->zoneInfo_.id;
      find_topzone = true;
    }

    if (!find_bottomzone || !find_topzone) {
      ++it;
    } else
      break;
  }
  if (if_debug) {
    if (!(find_bottomzone && find_topzone))
      std::cout << "in [dm_zone.cpp] [SwapZone] try to swap 2 zone's LBA2PBA "
              "mapping, but don't find the zones that need to be swapped"
           << std::endl;
  }
  // unlock

  // insert to corresponding level, level = erase count % number of levels
  std::shared_ptr<DmZone> bottom_zone_after_reset_ptr;
  bottom_zone_after_reset_ptr = *bottom_zone;
  int new_level = bottom_zone_after_reset_ptr->zoneInfo_.erase_count_ %
                  WEAR_LEVELING_WINDOW_SIZE;
  int old_level = (bottom_zone_after_reset_ptr->zoneInfo_.erase_count_ - 1) %
                  WEAR_LEVELING_WINDOW_SIZE;
  window_[new_level].push_back(bottom_zone_after_reset_ptr);
  // erase(it) return it++
  bottom_zone = window_[old_level].erase(bottom_zone);

  std::shared_ptr<DmZone> top_zone_after_reset_ptr;
  top_zone_after_reset_ptr = *top_zone;
  new_level = top_zone_after_reset_ptr->zoneInfo_.erase_count_ %
              WEAR_LEVELING_WINDOW_SIZE;
  old_level = (top_zone_after_reset_ptr->zoneInfo_.erase_count_ - 1) %
              WEAR_LEVELING_WINDOW_SIZE;
  window_[new_level].push_back(top_zone_after_reset_ptr);
  // erase(it) return it++
  top_zone = window_[old_level].erase(top_zone);
  return status;
}

Status DmZoneNamespace::GC(std::list<std::shared_ptr<DmZone>>::iterator it) {
  Status status;
  int toplevel = ((*it)->zoneInfo_.erase_count_) % WEAR_LEVELING_WINDOW_SIZE;
  int bottomlevel = (toplevel + 1) % WEAR_LEVELING_WINDOW_SIZE;
  int bottom_size = window_[bottomlevel].size();
  int top_size = window_[toplevel].size();

  while (bottom_size < 1) {
    // in case no zone in bottom level
    bottomlevel = (bottomlevel + 1) % WEAR_LEVELING_WINDOW_SIZE;
    bottom_size = window_[bottomlevel].size();
    if (toplevel == bottomlevel) {
      std::cout << "all zones are in top level, stop swap" << std::endl;
      status = Status::OK();
      return status;
    }
  }
  if (if_debug)
    std::cout << "toplevel: " << toplevel << " bottom level: " << bottomlevel
              << " topsize: " << top_size << " bottomsize: " << bottom_size
              << std::endl;

  auto bottom_it = window_[bottomlevel].begin();
  auto top_it = window_[toplevel].begin();

  // if bottom level only has one zone, migrate its data into the top zone, swap
  // their mapping & list level
  if (bottom_size == 1) {
    status = SwapZone(window_[bottomlevel].begin(), it);
    if (!status.ok()) {
      status = Status::NotFound(
          "[dm_zone.cpp] [GC] bottom level size is 1, fail to swap ");
      return status;
    }
  }
  // if bottom level has more than half of total zones, swap all zones in top
  // level, then increase all bottom zones erase count since all top zones will
  // be swapped, don't have to care which zone is the zone that out of the
  // window size
  else if (bottom_size > ZONEFile_NUMBER / 2.0) {
    int i = 0;
    for (; i < top_size; ++i) {
      std::cout << "bottom it:" << (*bottom_it)->zoneInfo_.id << std::endl;
      std::cout << "top it:" << (*top_it)->zoneInfo_.id << std::endl;

      status = SwapZone(bottom_it, top_it);
      if (!status.ok()) {
        status = Status::NotFound(
            "[dm_zone.cpp] [GC] bottom size > total/2, fail to swap");
        return status;
      }
      // bottom_it/top_it have been delete in SwapZone()
      // bottom_it++;
      // top_it++;
    }
    // after swap all top zone, insert all other bottom zones to the upper level

    for (; i < bottom_size; i++) {
      bottom_it = window_[bottomlevel].begin();
      (*bottom_it)->zoneInfo_.erase_count_++;
      window_[(bottomlevel + 1) % WEAR_LEVELING_WINDOW_SIZE].push_back(
          *bottom_it);
      window_[bottomlevel].erase(bottom_it);
      bottom_it++;
    }
  }
  // if bottom level has more than 1 but less than half of the total zones, swap
  // all zones in bottom level
  else {
    while (bottom_size > 0) {
      for (int i = 0; i < top_size && bottom_size > 0; i++) {
        status = SwapZone(bottom_it, top_it);
        if (!status.ok()) {
          status = Status::NotFound(
              "[dm_zone.cpp] [GC] 1< bottom size < total/2, fail to swap");
          return status;
        }
        // bottom_it++;
        bottom_it = window_[bottomlevel].begin();
        // top_it++;
        top_it = window_[toplevel].begin();

        bottom_size--;
      }
      toplevel = (toplevel - 1) % WEAR_LEVELING_WINDOW_SIZE;
      top_size = window_[toplevel].size();
      top_it = window_[toplevel].begin();
    }
  }
  if (if_debug) CheckWindow();
  return status;
}
/* call specific zone's reset ptr,
 * in window insert this zone into upper list
 * if out of window, call GC
 * if necessary, GC will call migrate*/
Status DmZoneNamespace::Resetptr(int id) {
  Status status;
  std::shared_ptr<Zone> dmzone = GetZone(id);
  // in window, reschedule this zone
  // window is a circular vector, level = erase count % # of levels
  int list_level = dmzone->zoneInfo_.erase_count_ % WEAR_LEVELING_WINDOW_SIZE;
  auto it = window_[list_level].begin();
  while (it != window_[list_level].end()) {
    if ((*it)->zoneInfo_.id == dmzone->zoneInfo_.id) {
      // find that zone in the list
      status = Status::OK();
      if (if_debug)
        std::cout << "[dm_zone.cpp] [Resetptr] find target zone [" << id
                  << "] in list level: " << list_level
                  << ", try to reschedule to upper list" << std::endl;
      break;
    }
    it++;
  }
  if (!status.ok()) {
    status =
        Status::NotFound("[dm_zone.cpp] [Resetptr] don't find target zone");
    return status;
  }
  // remove it from current list level, insert into upper level.
  // if out of window size, call GC
  if (list_level + 1 >= WEAR_LEVELING_WINDOW_SIZE) {
    // GC would call migrate, reschedule this zone to proper level
    if (if_debug)
      std::cout << "[dm_zone] [Resetptr] zone LBA [" << id << "], PBA ["
                << (*it)->zoneInfo_.id
                << "], trigger GC, current levelist: " << list_level
                << std::endl;
    status = GC(it);
    if (!status.ok()) {
      std::cout << status.ToString() << std::endl;
      status = Status::NotFound("[dm_zone.cpp] [Resetptr] GC failed");
      return status;
    }
  } else {
    // reset traget zone's write ptr
    // if trigger GC(), write ptr will be reset in GC()->SwapZone()
    dmzone->ResetWritePointer();
    // insert
    window_[list_level + 1].push_back(*it);
    // erase
    window_[list_level].erase(it);
    // dmzone->ResetWritePointer() already increase the erase count, no need to
    // increase again
  }
  return status;
}

Status DmZoneNamespace::InitZone(const char *Zonepath, const char *filename,
                                 char *filepath) {
  strcpy(filepath, Zonepath);
  if (filepath[strlen(Zonepath) - 1] != '/') strcat(filepath, "/");
  strcat(filepath, filename);
  printf("[Dm_zone.cpp] [InitZone] path is = %s\n", filepath);
  return Status::OK();
}
// delete all zone files left, create a new zone env
Status DmZoneNamespace::InitZNS(const char *dir_name) {
  // check if dir_name is a valid dir
  Status status;
  char filepath[256] = {0};
  struct stat s;
  lstat(dir_name, &s);
  if (!S_ISDIR(s.st_mode)) {
    // creat dir
    int success = mkdir(dir_name, S_IRWXU);
    if (success == 0) {
      std::cout
          << "[Dm_zone.cpp] [init_scanner] first time open this ZNS, create "
             "dir "
          << dir_name << std::endl;
      status = Status::OK();
    }
    if (success == -1) {
      std::cout
          << "[Dm_zone.cpp] [init_scanner] first time open this ZNS. However, "
             "create dir "
          << dir_name << " failed" << std::endl;
      status = Status::NotSupported("dirctory create filed");
    }
    return status;
  }
  // recover file list
  struct dirent *filename;  // return value for readdir()
  DIR *dir;                 // return value for opendir()
  dir = opendir(dir_name);
  if (NULL == dir) {
    std::cout << "[Dm_zone.cpp] [init_scanner] Can not open dir " << dir_name
              << std::endl;
    status = Status::NotFound("dirctory open filed");
    return status;
  }
  zones_.clear();
  while ((filename = readdir(dir)) != NULL) {
    InitZone(dir_name, filename->d_name, filepath);
    // get rid of "." and ".."
    if (strcmp(filename->d_name, ".") == 0 ||
        strcmp(filename->d_name, "..") == 0)
      continue;
    if (remove(filepath) == 0)
      printf("Removed %s.\n", filepath);
    else
      perror("remove fail");
    /*int zone_id = atol(filename->d_name);
    fstream fs;
    DmZone DmZone(fs);
    InitZone(filename->d_name, DmZone);
    zones_.emplace(zone_id,DmZone);*/
  }
  // for dmZNS, create wearleveling used window, record each zone's erase count
  for (int i = 0; i < WEAR_LEVELING_WINDOW_SIZE; i++) {
    std::list<std::shared_ptr<DmZone>> new_list;
    window_.push_back(new_list);
  }

  // recreate new zone files, file number is predefined
  for (int i = 0; i < ZONEFile_NUMBER; ++i) {
    status = NewZone();
    if (!status.ok()) {
      std::cout << status.ToString() << std::endl;
      status = Status::Corruption(
          "[hm_zone.cpp] [InitZNS] in ZNS Initialization, create new zone file "
          "failed, zone id: " +
          std::to_string(next_zone_id_));
      return status;
    }
  }
  status = Status::OK();
  return status;
}

DmZoneNamespace::DmZoneNamespace() { InitZNS(path.c_str()); }

Status DmZoneNamespace::CheckGC() {
  Status status;
  for (int i = 0; i < WEAR_LEVELING_WINDOW_SIZE + 1; ++i) {
    Resetptr(0);
  }
  std::shared_ptr<Zone> dmzone = GetZone(0);
  int list_level = dmzone->zoneInfo_.erase_count_ & WEAR_LEVELING_WINDOW_SIZE;
  auto it = window_[list_level].begin();
  while (it != window_[list_level].end()) {
    if ((*it)->zoneInfo_.id == dmzone->zoneInfo_.id) {
      // find that zone in the list
      std::cout << "in [dm_zone.cpp] [CheckGC] find target zone in list level: "
                << list_level << std::endl;
      status = Status::OK();
      break;
    }
    it++;
  }
  if (!status.ok()) {
    std::cout << "don't find target zone in window" << std::endl;
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
