#include "stdafx.h"
#include "producer1c.h"

using namespace KafkaExport::KafkaProducerc1C;
//---------------------------------------------------------------------------//
KafkaProducerCore::KafkaProducerCore()
{
	producer = nullptr;

	Init = false;
	partition = -1;
	try {
		conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	}
	catch (...) {
		conf = nullptr;
	}

	try {
		tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	}
	catch (...) {
		tconf = nullptr;
	}
	
}
//---------------------------------------------------------------------------//
KafkaProducerCore::~KafkaProducerCore()
{
	if (conf != nullptr){
		delete conf;
	}
	
	if (tconf != nullptr) {
		delete tconf;
	}
	
	if (producer != nullptr) {
		delete producer;
	}
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::Initialize(std::string _brokers, std::string _topic, int32_t _partition)
{
	RetValue res;

	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}

	if (tconf == nullptr) {
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}

	errstr.clear();
	Init = false;

	if (_brokers.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty broker address");
		return res;
	}

	if (_topic.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty topic");
		return res;
	}

	if (producer != nullptr) {
		delete producer;
	}
		
	if (partition == -1) {
		partition = RdKafka::Topic::PARTITION_UA;
	}

	brokers = _brokers;
	topic = _topic;
	partition = _partition;

	res = GlobalConfDefaultInit(brokers);
	if (!res.succes) {
		return res;
	}
	
	producer = RdKafka::Producer::create(conf, errstr);
	if ((!!producer) & (errstr.empty())) {
		Init = true;
	}
	else {
		res.error = err(ERR_UNHANDLED, errstr);
	}
	
	res.succes = Init;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::ConfReset()
{
	RetValue res;

	if (conf != nullptr) {
		delete conf;
	}

	if (tconf != nullptr) {
		delete tconf;
	}

	try {
		conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	}
	catch (...) {
		conf = nullptr;
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}

	try {
		tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	}
	catch (...) {
		tconf = nullptr;
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}

	res = GlobalConfDefaultInit(brokers);
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::GlobalConfDefaultInit(std::string _brokers)
{
	RetValue res;

	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}

	if (tconf == nullptr) {
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}

	RdKafka::Conf::ConfResult ConfSetResult;

	ConfSetResult = conf->set("default_topic_conf", tconf, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	ConfSetResult = conf->set("dr_cb", &dr_cb, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	if (!_brokers.empty()) {
		ConfSetResult = conf->set("metadata.broker.list", brokers, errstr);
		if (ConfSetResult != RdKafka::Conf::CONF_OK) {
			res.error = err(ERR_UNHANDLED, errstr);
		}
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
bool KafkaProducerCore::IsInit()
{
	return Init;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::SetGlobalConf(std::string key, std::string value)
{
	RetValue res;

	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf error");
		return res;
	}
	
	if (key.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty key");
		return res;
	}
	
	if (value.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty value");
		return res;
	}

	RdKafka::Conf::ConfResult ConfSetResult;
	ConfSetResult = conf->set(key, value, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::SetTopicConf(std::string key, std::string value)
{
	RetValue res;

	if (tconf == nullptr) {
		res.error = err(ERR_UNHANDLED, "tconf error");
		return res;
	}
	
	if (key.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty key");
		return res;
	}
	
	if (value.empty()) {
		res.error = err(ERR_BADPARAMETR, "empty value");
		return res;
	}

	RdKafka::Conf::ConfResult ConfSetRes = tconf->set(key, value, errstr);
	if (ConfSetRes != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaProducerCore::Produce(KafkaExport::DataConversion::DataConteiner* json_conteiner)
{
	RetValue res;
	if (!IsInit()){
		res.error = err(ERR_NOTINIT);
		return res;
	}
	if (!json_conteiner->IsValid()) {
		res.error = err(ERR_JSONPARSING, "invalid json");
		return res;
	}
	dr_cb.ClearRecords();

	std::string MessageKey;
	std::string MessageValue;
	RdKafka::Headers* headers = nullptr;

	for (unsigned int i = 0; i < json_conteiner->ElementsCount(); ++i) {

		MessageKey.clear();
		MessageValue.clear();
		headers = nullptr;
		try {
			if (!json_conteiner->GetKeyByIndex(i, &MessageKey)) {
				throw 0;
			}
			if (!json_conteiner->GetValueByIndex(i, &MessageValue)) {
				throw 0;
			}
			if (json_conteiner->HeadersContains(i)) {
				headers = RdKafka::Headers::create();
				if (!json_conteiner->GetHeadersByIndex(i, headers)) {
					throw 0;
				}
			}
		}
		catch (...) {
			if (headers != nullptr) {
				delete headers;
				headers = nullptr;
			}
			res.error = err(ERR_JSONPARSING, "json parsing error");
			return res;
		}

		RdKafka::ErrorCode resp =
			producer->produce(topic, partition,
				RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
				/* Value */
				(char*)MessageValue.c_str(), MessageValue.size(),
				/* Key */
				(char*)MessageKey.c_str(), MessageKey.size(),
				/* Timestamp (defaults to now) */
				0,
				/* Message headers, if any */
				headers,
				/* Per-message opaque value passed to
				* delivery report */
				nullptr);

		if (resp != RdKafka::ERR_NO_ERROR) {

			delete headers; /* Headers are automatically deleted on produce * success. */
			headers = nullptr;

			RetValue DeliveryRes = dr_cb.AddRecord(MessageKey, "Error", topic, partition, -1, -1, RdKafka::err2str(resp));
			if (!DeliveryRes.succes) {
				res.error = err(ERR_BADALLOC, "delivery report generation error");
				return res;
			}
		}
	}
	while (producer->outq_len() > 0) {
		producer->poll(1000);
	}

	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::BoolValueResult KafkaProducerCore::IsDelivered()
{
	BoolValueResult res;
	if (!IsInit()){
		res.error = err(ERR_NOTINIT);
		return res;
	}
	
	bool delivered = true;
	for (unsigned int i = 0; i < dr_cb.records.size(); ++i) {
		if (dr_cb.records[i].status != "Persisted") {
			delivered = false;
			break;
		}
	}
	res.value = delivered;
	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
