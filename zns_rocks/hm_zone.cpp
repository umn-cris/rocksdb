//
// Created by Wei00161@umn.edu on 2020/5/16.
//

#include <sys/stat.h>
#include <dirent.h>
#include "hm_zone.h"

using namespace std;
using namespace leveldb;
string path = "hm_zones_";
/*HmZone::HmZone(fstream& fs,size_t id):modify_zone_(fs){
    //zone_info_ = (struct ZoneInfo*)malloc(sizeof(struct ZoneInfo));
    zoneInfo_.id = id;
    zoneInfo_.write_pointer = 0;
    zoneInfo_.zone_type = SEQUENTIAL_WRITE_REQUIRED;
    zoneInfo_.zone_condition = CLOSED;
    zoneInfo_.zone_state = SEQUENTIAL;
    modify_zone_.open(path+"/"+to_string(zoneInfo_.id),ios::out);
    if(!modify_zone_.is_open()) cout<<"[hm_zone.cpp] [HmZone] create zone file failed"<<endl;
    //modify_zone_<<ToString()<<endl;
    modify_zone_.close();
    //if(if_debug) cout<<"create zone "<<zoneInfo_.id<<endl;
}*/
HmZone::HmZone(fstream& fs,size_t id){
    //zone_info_ = (struct ZoneInfo*)malloc(sizeof(struct ZoneInfo));
    zoneInfo_.id = id;
    zoneInfo_.write_pointer = 0;
    zoneInfo_.zone_type = SEQUENTIAL_WRITE_REQUIRED;
    zoneInfo_.zone_condition = CLOSED;
    zoneInfo_.zone_state = SEQUENTIAL;
    fstream modify_zone_;
    modify_zone_.open(path+"/"+to_string(zoneInfo_.id),ios::out);
    if(!modify_zone_.is_open()) cout<<"[hm_zone.cpp] [HmZone] create zone file failed"<<endl;
    //modify_zone_<<ToString()<<endl;
    modify_zone_.close();
    //if(if_debug) cout<<"create zone "<<zoneInfo_.id<<endl;
}

// for correctness, OpenZone only change zone state. Zone file will only be open when data will be written or read from zone file
Status HmZone::OpenZone() {
    Status status;
    /*modify_zone_.open(path+"/"+to_string(zoneInfo_.id), ios::out|ios::in);
    if(!modify_zone_.is_open()){
        status = Status::NotFound("[hm_zone.cpp] [OpenZone] Open zone failed, zone id: "+ to_string(zoneInfo_.id));
        return status;
    }*/
    zoneInfo_.zone_condition = OPEN;
    status = Status::OK();
    if(if_debug) cout<<"open zone "<<zoneInfo_.id<<endl;
    return status;
}

Status HmZone::FinishZone() {
    Status status;
    zoneInfo_.write_pointer = ZONESIZE;
    zoneInfo_.size = ZONESIZE;
    status = Status::OK();
    if(if_debug) cout<<"finishe zone "<<zoneInfo_.id<<endl;
    return status;
}

ZoneInfo HmZone::ReportZone() {
    if(if_debug) cout<<"report zone "<<zoneInfo_.id<<endl;
    return zoneInfo_;
}

// should not call it via shared ptr, should use zns to call
Status HmZone:: ResetWritePointer(){
    Status status;
    zoneInfo_.write_pointer = 0;
    zoneInfo_.size = 0;
    zoneInfo_.erase_count_++;
    //modify_zone_.seekp(0,ios::beg);
    //modify_zone_<<ToString()<<endl;
    // no close here!
    status = Status::OK();
    if(if_debug) cout<<"in [hm_zone] [reset write pointer] "<<zoneInfo_.id<<endl;
    return status;
}

//same reason with OpenZone, when finish writing or read  data from zone file, it will be closed rather than here
Status HmZone::CloseZone() {
    Status status;
    zoneInfo_.zone_condition=CLOSED;
    //modify_zone_.seekp(0,ios::beg);
    //modify_zone_<<ToString()<<endl;
    //modify_zone_.close();
    status = Status::OK();
    if(if_debug) cout<<"close zone "<<zoneInfo_.id<<endl;
    return status;
}

Status HmZone::ZoneWrite(ZoneAddress addr, const char *data) {
    Status status;
    // to write data in zone file, first open it
    // only change zone state, open operation will be handle manually in next line
    // this will help avoid unexpected bug (i.e., avoid opening a file both for read and write, and reset read/write pointer at the same time)
    status = OpenZone();
    if(zoneInfo_.write_pointer+addr.length > ZONESIZE){
        status = Status::InvalidArgument("[hm_zone.cpp] [ZoneWrite] WriteZone failed, reach end of zone file, zone id: "+ to_string(zoneInfo_.id));
        return status;
    }
    fstream modify_zone_;
    //only "ios::out" rather than "ios::out|ios::in"
    modify_zone_.open(path+"/"+to_string(zoneInfo_.id),ios::out|ios::binary);
    if(!modify_zone_.is_open()){
        status = Status::NotFound("[hm_zone.cpp] [ZoneWrite] OpenZone failed, zone id: "+ to_string(zoneInfo_.id));
        return status;
    }

    // if comes a non-sequential write request
    if(addr.offset != zoneInfo_.write_pointer){
        //for a sequential required zone,
        if(zoneInfo_.zone_type == SEQUENTIAL_WRITE_REQUIRED){
            cout<<""<<endl;
            status.NotSupported("[hm_zone.cpp] [ZoneWrite] it is not a sequential write, sequential write required. Zone id: " + to_string(addr.zone_id));
            return status;
        }
        else if(zoneInfo_.zone_type == SEQUENTIAL_WRITE_PREFERRED){
            zoneInfo_.zone_state = NON_SEQUENTIAL;
        }
        zoneInfo_.write_pointer = addr.offset;
    }
    modify_zone_.seekp(0);
    //if(modify_zone_.tellp() != addr.offset) modify_zone_.seekp(addr.offset);
    modify_zone_.write(data,addr.length);
    if(!modify_zone_.good()){
        if(modify_zone_.fail())
            status = Status::InvalidArgument("[hm_zone.cpp] [ZoneWrite] WriteZone failed, no remain space / write mode error / format error... zone id: "+ to_string(zoneInfo_.id));
        else
            status = Status::InvalidArgument("[hm_zone.cpp] [ZoneWrite] WriteZone failed, unknown reasons, need further checking. zone id: "+ to_string(zoneInfo_.id));
    }
    modify_zone_.close();
    return status;
}


Status HmZone::ZoneRead(ZoneAddress addr, char *data) {
    Status status;
    fstream modify_zone_;
    status = OpenZone();
    modify_zone_.open(path+"/"+to_string(zoneInfo_.id),ios::in|ios::binary);

    if(!modify_zone_.is_open()){
        status = Status::NotFound("[hm_zone.cpp] [ZoneRead] OpenZone failed, zone id: "+ to_string(zoneInfo_.id));
        return status;
    }

    modify_zone_.seekg(addr.offset);
    modify_zone_.read(data,addr.length);
    if(!modify_zone_.good()){
        if(modify_zone_.eof())
            status = Status::InvalidArgument("[hm_zone.cpp] [ZoneRead] ReadZone failed, reach end of zone file, zone id: "+ to_string(zoneInfo_.id));
        else if(modify_zone_.fail())
            status = Status::InvalidArgument("[hm_zone.cpp] [ZoneRead] ReadZone failed, no remain space / read mode error / format error... zone id: "+ to_string(zoneInfo_.id));
    }
    modify_zone_.close();
    return status;
}

/*Status HmZoneNamespace::Write(ZoneAddress addr, const char *data) {
    Status status;
    fstream fs;
    HmZone write_zone(fs);

    // get target zone
    status = GetZone(addr.zone_id,write_zone);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[hm_zone.cpp] [Write] in ZNS write, GetZone failed, zone id: " + to_string(addr.zone_id));
        return status;
    }

    // call target zone's zone write function
    status = write_zone.ZoneWrite(addr,data);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[hm_zone.cpp] [Write] in ZNS write, WriteZone failed, zone id: " + to_string(addr.zone_id));
        return status;
    }
    return status;
}*/

/*Status HmZoneNamespace::Read(ZoneAddress addr,  char *data) {
    Status status;
    fstream fs;
    HmZone read_zone(fs);

    // get target zone
    status = GetZone(addr.zone_id,read_zone);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[hm_zone.cpp] [Read] in ZNS read, GetZone failed, zone id: " + to_string(addr.zone_id));
        return status;
    }

    // call target zone's zone write function
    status = read_zone.ZoneRead(addr,data);
    if(!status.ok()) {
        cout<<status.ToString()<<endl;
        status = Status::NotFound("[hm_zone.cpp] [Read] in ZNS read, ReadZone failed, zone id: " + to_string(addr.zone_id));
        return status;
    }
    return status;
}*/

// hm zone does not need any other operation, just record the erase count
Status HmZoneNamespace::Resetptr(int id){
    Status status;
    shared_ptr<Zone> hmzone = GetZone(id);
    status = hmzone->ResetWritePointer();
    return status;
}



Status HmZoneNamespace::NewZone(){
    Status status;
    fstream fs;
    //begin: modification for shared_ptr
    shared_ptr<HmZone> zone_ptr(new HmZone(fs,next_zone_id_));
    auto it = zones_.emplace(next_zone_id_, zone_ptr);
    //end
    if(!it.second){
        status = Status::InvalidArgument("[hm_zone.cpp] [NewZone] already has a zone in ZNS, zone id: "+to_string(next_zone_id_));
        return status;
    }
    ++next_zone_id_;
    status = Status::OK();
    return status;
}

// a filepath completion function, used in "InitZNS"
Status HmZoneNamespace:: InitZone(const char *path, const char *filename,  char *filepath){
    strcpy(filepath, path);
    if(filepath[strlen(path) - 1] != '/')
        strcat(filepath, "/");
    strcat(filepath, filename);
    //printf("[hm_zone.cpp] [InitZone] path is = %s\n",filepath);
    return Status::OK();
}
//delete all zone files left, create a new zone env (creat zone file, file number is an adjustable parameter)
Status HmZoneNamespace::InitZNS(const char * dir_name){
    // check if dir_name is a valid dir
    Status status;
    char filepath[256] = {0};
    struct stat s;
    lstat( dir_name , &s );
    if( ! S_ISDIR( s.st_mode ) )
    {
        //creat dir
        int success = mkdir(dir_name, S_IRWXU);
        if(success == 0) {
            cout<<"[hm_zone.cpp] [init_scanner] first time open this ZNS, create dir "<<dir_name<<endl;
            status = Status::OK();
        }
        if(success == -1){
            cout<<"[hm_zone.cpp] [init_scanner] first time open this ZNS. However, create dir "<<dir_name<<" failed"<<endl;
            status = Status::NotSupported("dirctory create filed");
        }
        return status;
    }
    // recover file list
    struct dirent * filename;    // return value for readdir()
    DIR * dir;                   // return value for opendir()
    dir = opendir( dir_name );
    if( NULL == dir )
    {
        cout<<"[hm_zone.cpp] [init_scanner] Can not open dir "<<dir_name<<endl;
        status = Status::NotFound("dirctory open filed");
        return status;
    }
    zones_.clear();
    while( ( filename = readdir(dir) ) != NULL )
    {
        InitZone(dir_name, filename->d_name, filepath);
        // get rid of "." and ".."
        if( strcmp( filename->d_name , "." ) == 0 ||
            strcmp( filename->d_name , "..") == 0    )
            continue;
        if( remove(filepath) == 0 ){
            //printf("Removed %s.\n", filepath);
        }
        else
            perror("remove fail");
        /*int zone_id = atol(filename->d_name);
        fstream fs;
        HmZone hmZone(fs);
        InitZone(filename->d_name, hmZone);
        zones_.emplace(zone_id,hmZone);*/
    }
    // recreate new zone files, file number is predefined
    for (int i = 0; i < ZONEFile_NUMBER; ++i) {
        status = NewZone();
        if(!status.ok()) {
            cout<<status.ToString()<<endl;
            status = Status::Corruption("[hm_zone.cpp] [InitZNS] in ZNS Initialization, create new zone file failed, zone id: " + to_string(next_zone_id_));
            return status;
        }
    }
    status = Status::OK();
    return status;
}

HmZoneNamespace::HmZoneNamespace() {
    InitZNS(path.c_str());
}


