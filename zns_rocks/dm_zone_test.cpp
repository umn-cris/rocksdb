//
// Created by Wei00161@umn.edu on 2020/5/14.
//

#include "leveldb/db.h"
#include <cassert>
#include <iostream>
#include "zone_namespace.h"
#include "dm_zone.h"
using namespace std;
using namespace leveldb;

int main() {
    cout<<"~~~~~~create DmZoneNamespace~~~~~~~~"<<endl;
    shared_ptr<DmZoneNamespace> dmzonenamespace = DmZoneNamespace::CreatZoneNamespace();


    cout<<"~~~~~~write~~~~~~~~"<<endl;
    for (int i = 0; i < ZONEFile_NUMBER; ++i) {
        shared_ptr<Zone> dmzone = dmzonenamespace->GetZone(i);

        ZoneAddress zoneAddress;
        zoneAddress.zone_id = i;
        zoneAddress.offset = 0;
        string str = "that is test"+to_string(i);
        zoneAddress.length = str.size();
        dmzone->ZoneWrite(zoneAddress,str.c_str());
    }

    dmzonenamespace->CheckGC();

    cout<<"~~~~~~~~~~~read~~~~~~~~~~~"<<endl;
    for (int i = 0; i < ZONEFile_NUMBER; ++i) {
        shared_ptr<Zone> dmzone = dmzonenamespace->GetZone(i);

        ZoneAddress zoneAddress;
        zoneAddress.zone_id = i;
        zoneAddress.offset = 0;
        zoneAddress.length = dmzone->zoneInfo_.size;
        char result[dmzone->zoneInfo_.size];
        dmzone->ZoneRead(zoneAddress,result);
        cout<<result<<endl;
    }




    return 0;
}
