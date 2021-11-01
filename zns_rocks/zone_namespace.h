#ifndef ZONE_NAMESPACE_H_
#define ZONE_NAMESPACE_H_

#include <vector>
#include <unordered_map>
#include <string>
#include <iostream>
#include <list>
#include "leveldb/status.h"
#include <memory>
//#include <tr1/memory>
//#include <bits/shared_ptr.h>
static const size_t ZONESIZE = 512*1024*1024;
const bool if_debug = true;
const long ZONEFile_NUMBER = 5;
namespace leveldb {

enum ZoneType {CONVENTIONAL, SEQUENTIAL_WRITE_PREFERRED, SEQUENTIAL_WRITE_REQUIRED};
static const std::string TypeStr[] = { "CONVENTIONAL", "SEQUENTIAL_WRITE_PREFERRED", "SEQUENTIAL_WRITE_REQUIRED" };
enum ZoneCondition {OPEN, CLOSED};
static const std::string ConditionStr[] = { "OPEN", "CLOSED"};
enum ZoneState {SEQUENTIAL, NON_SEQUENTIAL};
static const std::string StateStr[] = { "SEQUENTIAL", "NON_SEQUENTIAL"};


struct ZoneAddress {
    int zone_id;
    size_t offset; // unit: bytes
    size_t length = 0; // unit: bytes
};

struct ZoneInfo {
    int id = 0;
    size_t first_LBA = 0;
    size_t size = 0;
    size_t write_pointer = 0;
    size_t erase_count_ = 0;
    ZoneType zone_type;
    ZoneCondition zone_condition;
    ZoneState zone_state;
};


class Zone {
public:
    Zone()= default;
    virtual ~Zone()= default;



    // lookup file to open
    // if not exist, create
    // file name is "Zone" + id
    virtual Status OpenZone()=0;

    virtual Status CloseZone()=0;

    virtual Status FinishZone()=0;

    virtual ZoneInfo ReportZone()=0;

    virtual Status ResetWritePointer()=0;

    // virtual Status Read()=0;
    virtual Status ZoneRead(ZoneAddress addr,  char* data) = 0;

    // virtual Status Write()=0;
    virtual Status ZoneWrite(ZoneAddress addr, const char* data) = 0;

    ZoneInfo zoneInfo_;
protected:

    //ZoneInfo* zone_info_;
};




class ZoneNamespace {
public:
    ZoneNamespace()= default;
    virtual ~ZoneNamespace()= default;

    // Get the number of zones.
    virtual size_t GetZoneCount() = 0;

    // Create a new zone.
    virtual Status NewZone() = 0;

    // Remove an existing zone based on its id.
    virtual Status RemoveZone(int id) = 0;

    //virtual Status GetZone(int id, std::shared_ptr<Zone>* zone_ptr) = 0;
    virtual std::shared_ptr<Zone> GetZone(int id) = 0;

    virtual Status Resetptr(int id) = 0;
    virtual Status InitZNS(const char* dir_name) = 0;

    virtual Status InitZone(const char *path, const char *filename,  char *filepath) = 0;

    //virtual Status Write(ZoneAddress addr, const char* data) = 0;
    //virtual Status Read(ZoneAddress addr,  char* data) = 0;

    // virtual Status Write(ZoneAddress addr, string content) = 0;
};

}

#endif // ZONE_NAMESPACE_H_
