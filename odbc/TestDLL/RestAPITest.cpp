#include "Tests.h"

void restAPITest() {
    {
        bool ret = restAuthenticate ( KServerAddr, KPort, KUserName, KPassword );
        
        if ( !ret )
        { report(); }
    }
    {
        std::vector<string> holder;
        restListProjects ( KServerAddr, KPort, KUserName, KPassword, holder );
        
        if ( holder.size() == 0 )
        { report(); }
    }
    {
        std::unique_ptr<MetadataResponse> ret = restGetMeta ( KServerAddr, KPort, KUserName, KPassword, KDefaultProject );
    }
}