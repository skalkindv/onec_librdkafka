#include "stdafx.h"
#include "consumer1c.h"

using namespace KafkaExport::KafkaConsumer1C;
//---------------------------------------------------------------------------//
KafkaConsumerCore1C::MyRebalanceCb::MyRebalanceCb()
{

}
//---------------------------------------------------------------------------//
KafkaConsumerCore1C::MyRebalanceCb::~MyRebalanceCb()
{
	
}
//---------------------------------------------------------------------------//
void KafkaConsumerCore1C::MyRebalanceCb::rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*>& partitions)
{
    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
        consumer->assign(partitions);
    }
    else {
        consumer->unassign();
    }
}
//---------------------------------------------------------------------------//