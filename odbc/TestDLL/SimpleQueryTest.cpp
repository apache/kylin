#include "Tests.h"

void simpleQueryTest() {
    //Intercept query test
    {
        std::unique_ptr<SQLResponse> y  = restQuery ( L"SELECT 1", KServerAddr, KPort, KUserName, KPassword, KDefaultProject );
        
        if ( ( int ) y->results.size() != 1 ) {
            report();
        }
    }
    //Ungzipped Query Test
    {
        std::unique_ptr<SQLResponse> y  = restQuery ( L"select cal_dt from test_kylin_fact limit 1", KServerAddr, KPort,
                                                      KUserName, KPassword, KDefaultProject );
                                                      
        if ( ( int ) y->results.size() != 1 ) {
            report();
        }
    }
    //zipped Query Test
    {
        std::unique_ptr<SQLResponse> y  = restQuery ( L"select * from test_kylin_fact limit 12", KServerAddr, KPort, KUserName,
                                                      KPassword, KDefaultProject );
                                                      
        if ( ( int ) y->results.size() != 12 ) {
            report();
        }
    }
}