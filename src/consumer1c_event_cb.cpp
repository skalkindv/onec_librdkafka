#include <fstream>
#include <iostream>
#include <string>
#include <stdio.h>
#include <time.h>
#include "stdafx.h"
#include "consumer1c.h"

using namespace KafkaExport::KafkaConsumer1C;

const std::string currentDateTime() {
    time_t     now = time(0);
    struct tm  tstruct;
    char       buf[80];
    tstruct = *localtime(&now);
    // Visit http://en.cppreference.com/w/cpp/chrono/c/strftime
    // for more information about date/time format
    strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);

    return buf;
}

//---------------------------------------------------------------------------//
KafkaConsumerCore1C::MyEventCb::MyEventCb()
{

}
//---------------------------------------------------------------------------//
KafkaConsumerCore1C::MyEventCb::~MyEventCb()
{

}
//---------------------------------------------------------------------------//
void KafkaConsumerCore1C::MyEventCb::event_cb(RdKafka::Event& event)
{

    if (!enabled) {
        return;
    }

    std::ofstream log_file;
    try{
        log_file.open(log_path.c_str(), std::ios_base::app);
    }
    catch (...) {
        return;
    }

    log_file << "[" << currentDateTime() << "] - ";
    switch (event.type())
    {

    case RdKafka::Event::EVENT_ERROR:
        if (event.fatal()) {
            log_file << "FATAL ";

        }
        log_file << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;

    case RdKafka::Event::EVENT_STATS:
        log_file << "\"STATS\": " << event.str() << std::endl;
        break;

    case RdKafka::Event::EVENT_LOG:

        log_file << "LOG-" << event.severity() << "-" << event.fac().c_str() << ": " << event.str().c_str() << std::endl;
        break;

    case RdKafka::Event::EVENT_THROTTLE:
        log_file << "THROTTLED: " << event.throttle_time() << "ms by " <<
            event.broker_name() << " id " << (int)event.broker_id() << std::endl;
        break;

    default:
        log_file << "EVENT " << event.type() <<
            " (" << RdKafka::err2str(event.err()) << "): " <<
            event.str() << std::endl;
        break;
    }

    try {
        log_file.close();
    }
    catch (...){
        return;
    }
}
//---------------------------------------------------------------------------//
bool KafkaConsumerCore1C::MyEventCb::enable(std::string Path)
{
    if (Path.empty()) {
        return false;
    }
    log_path = Path;
    enabled = true;

    return true;
}
//---------------------------------------------------------------------------//
void KafkaConsumerCore1C::MyEventCb::disable()
{
    enabled = false;
    log_path.clear();
}
//---------------------------------------------------------------------------//
