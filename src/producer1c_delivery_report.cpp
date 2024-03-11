#include "stdafx.h"
#include "producer1c.h"

using namespace KafkaExport::KafkaProducerc1C;
//---------------------------------------------------------------------------//
KafkaProducerCore::MyDeliveryReportCb::MyDeliveryReportCb()
{

}
//---------------------------------------------------------------------------//
KafkaProducerCore::MyDeliveryReportCb::~MyDeliveryReportCb()
{
	ClearRecords();
}
//---------------------------------------------------------------------------//
void KafkaProducerCore::MyDeliveryReportCb::dr_cb(RdKafka::Message &message) {
	std::string status_name;
	switch (message.status())
	{
	case RdKafka::Message::MSG_STATUS_NOT_PERSISTED:
		status_name = "NotPersisted";
		break;
	case RdKafka::Message::MSG_STATUS_POSSIBLY_PERSISTED:
		status_name = "PossiblyPersisted";
		break;
	case RdKafka::Message::MSG_STATUS_PERSISTED:
		status_name = "Persisted";
		break;
	default:
		status_name = "Unknown?";
		break;
	}

	RetValue res = AddRecord(*(message.key()), status_name, message.topic_name(), message.partition(), message.timestamp().timestamp, message.offset(), message.errstr());
	//FIXME: error process
}
//---------------------------------------------------------------------------//
void KafkaProducerCore::MyDeliveryReportCb::ClearRecords() {
	records.clear();
	//records.shrink_to_fit();
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::MyDeliveryReportCb::AddRecord(std::string key, std::string status, std::string topic, int32_t partition, int64_t timestamp, int64_t offset, std::string error) {

	RetValue Res;
	delivery_record rec;

	rec.key = key;
	rec.status = status;
	rec.topic = topic;
	rec.partition = partition;
	rec.timestamp = timestamp;
	rec.offset = offset;
	rec.error = error;

	try {
		records.push_back(rec);
	}
	catch (...) {
		Res.error = err(ERR_BADALLOC, "records.push_back error");
	}

	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
