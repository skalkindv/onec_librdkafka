#include "stdafx.h"
#include "consumer1c.h"
#include "gettime.h"

using namespace KafkaExport::KafkaConsumer1C;
//---------------------------------------------------------------------------//
KafkaConsumerCore1C::KafkaConsumerCore1C()
{
	consumer = nullptr;
	Init = false;

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
KafkaConsumerCore1C::~KafkaConsumerCore1C()
{

	if (conf != nullptr) {
		delete conf;
	}
	if (tconf != nullptr) {
		delete tconf;
	}

	ClearLocalQueue();
	if (consumer != nullptr) {
		consumer->close();
		delete consumer;
		//RdKafka::wait_destroyed(5000);
	}
	ClearCurrentAssignmentsList();
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::Initialize(std::string _brokers, std::string _groupId)
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

	try {
		local_queue.reserve(max_messages_in_local_queue);
	}
	catch(...)
	{
		res.error = err(ERR_BADALLOC, "local_queue.reserve");
		return res;
	}

	errstr.clear();
	Init = false;

	if (consumer != nullptr) {
		consumer->close();
		delete consumer;
		//RdKafka::wait_destroyed(5000);
	}

	if (_brokers.empty()) {
		res.error = err(ERR_UNHANDLED, "brokers addresses is empty");
		return res;
	}

	if (_groupId.empty()) {
		res.error = err(ERR_UNHANDLED, "group_id is empty");
		return res;
	}

	brokers = _brokers;
	groupId = _groupId;
	res = GlobalConfDefaultInit(brokers, groupId);
	if (!res.succes) {
		return res;
	}

	if (!log_path.empty()) {
		if (event_cb.enable(log_path) != true) {
			res.error = err(ERR_BADPARAMETR, "invalid log path");
			return res;
		}
	}
	else {
		event_cb.disable();
	}

	consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if ((!!consumer) & (errstr.empty())) {
		Init = true;
	}
	else {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	res.succes = Init;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::ConfReset()
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

	res = GlobalConfDefaultInit(brokers, groupId);
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::GlobalConfDefaultInit(std::string _brokers, std::string _groupId)
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

	ConfSetResult = conf->set("rebalance_cb", &rb_cb, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	ConfSetResult = conf->set("event_cb", &event_cb, errstr);
	if (ConfSetResult != RdKafka::Conf::CONF_OK) {
		res.error = err(ERR_UNHANDLED, errstr);
	}

	if (!_brokers.empty()) {
		ConfSetResult = conf->set("metadata.broker.list", _brokers, errstr);
		if (ConfSetResult != RdKafka::Conf::CONF_OK) {
			res.error = err(ERR_UNHANDLED, errstr);
		}
	}

	if (!_groupId.empty()) {
		ConfSetResult = conf->set("group.id", _groupId, errstr);
		if (ConfSetResult != RdKafka::Conf::CONF_OK) {
			res.error = err(ERR_UNHANDLED, errstr);
		}
	}
	else{
		res.error = err(ERR_BADPARAMETR, "empty group_id");
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
bool KafkaConsumerCore1C::IsInit()
{
	return Init;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::SetGlobalConf(std::string key, std::string value)
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
KafkaExport::RetValue KafkaConsumerCore1C::SetTopicConf(std::string key, std::string value)
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
KafkaExport::RetValue KafkaConsumerCore1C::Assign(std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList)
{
	RetValue res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}

	res = Unassign();
	if (!res.succes) {
		return res;
	}

	TopicPartitionDescription* ktp = nullptr;
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		if (ktp->topic.empty()){
			res.error = err(ERR_BADPARAMETR, "empty topic in topic-partition list");
			return res;
		}
		if (ktp->partition < 0){
			res.error = err(ERR_BADPARAMETR, "invalid partition in topic-partition list");
			return res;
		}
	}
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {

		RdKafka::TopicPartition* tp;
		ktp = &TopicPartitionList->at(i);

		try {
			if (ktp->offset < 0) {
				tp = RdKafka::TopicPartition::create(ktp->topic, ktp->partition);
			}
			else {
				tp = RdKafka::TopicPartition::create(ktp->topic, ktp->partition, ktp->offset);
			}
			TopicPatrtitionCurrentAssignments.push_back(tp);
		}
		catch (...) {
			res.error = err(ERR_BADALLOC, "TopicPatrtitionCurrentAssignments.push_back error");
			return res;
		}
	}

	RdKafka::ErrorCode error = consumer->assign(TopicPatrtitionCurrentAssignments);
	if (error) {
		res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
		return res;
	}

	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::Unassign()
{
	RetValue res;
	res.succes = true;

	if (IsInit()) {
		RdKafka::ErrorCode error = consumer->unassign();

		if (error) {
			res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
			res.succes = false;
		}
	}

	ClearCurrentAssignmentsList();
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::Subscribe(std::vector<std::string>* Topics)
{
	RetValue res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}
	
	for(size_t i = 0; i < Topics->size(); ++i){
		if (Topics->at(i).empty()){
			res.error = err(ERR_BADPARAMETR, "empty topic in topic list");
			return res;	
		}
	}

	RdKafka::ErrorCode error = consumer->subscribe(*Topics);
	if (error) {
		res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
		return res;
	}

	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::Unsubscribe()
{
	RetValue res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}

	RdKafka::ErrorCode error = consumer->unsubscribe();
	if (error) {
		res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
		return res;
	}
	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::Commit()
{
	RetValue res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}

	RdKafka::ErrorCode error = consumer->commitAsync();
	if (error) {
		res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
		return res;
	}
	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
bool KafkaConsumerCore1C::AddMessage(RdKafka::Message* message)
{
	try {
		local_queue.push_back(message);
	}
	catch (...) {
		return false;
	}
	return true;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::Consume(unsigned int timeout)
{
	RetValue res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}

	if (local_queue.size() >= max_messages_in_local_queue) {
		res.error = err(ERR_EMPTY, "local queue is full");
		return res;
	}

	RdKafka::Message* msg = consumer->consume(timeout);
	bool error = true;
	switch (msg->err()) {

	case RdKafka::ERR_NO_ERROR:
		if (!AddMessage(msg)) {
			delete msg;
			res.error = err(ERR_CONSUMPTION, "local queue writing error", true);
			return res;
		}
		error = false;
		break;
	case RdKafka::ERR__PARTITION_EOF:
		res.error = err(ERR_EOF, "EOF");
		break;
	case RdKafka::ERR__TIMED_OUT:
		res.error = err(ERR_EMPTY, msg->errstr());
		break;
	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		res.error = err(ERR_CONSUMPTION, msg->errstr());
		break;
	default:
			/* Errors */
		res.error = err(ERR_UNHANDLED, msg->errstr());
		break;
	}
	res.succes = !error;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::ConsumePool(int32_t count, int32_t errors_count_to_interrupt,  unsigned int timeout)
{
	RetValue res;
	
	if (count <= 0) {
		res.error = err(ERR_BADPARAMETR, "invalid messages count");
		return res;
	}
	
	if (errors_count_to_interrupt <= 0) {
		res.error = err(ERR_BADPARAMETR, "invalid interrupt errors count");
		return res;
	}
	
	if (timeout <= 0) {
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		return res;
	}
	
	int32_t error_count = 0;
	int64_t end = now() + timeout;
	int64_t remaining_timeout = timeout;

	for (int i = 0; i < count; ++i) {
		res = Consume((unsigned int)remaining_timeout);
		if (res.error.fatal) {
			return res;
		}
		if (!res.succes) {
			++error_count;
		}
		if (error_count >= errors_count_to_interrupt) {
			break;
		}
		remaining_timeout = end - now();
		if (remaining_timeout < 0) {
			break;
		}
	}
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetOffsets KafkaConsumerCore1C::QueryWatermarkOffsets(std::string topic, int32_t partition, int32_t timeoutms)
{
	RetOffsets res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}
	
	if (topic.empty()){
		res.error = err(ERR_BADPARAMETR, "empty topic");
		return res;	
	}
	
	if (partition < 0){
		res.error = err(ERR_BADPARAMETR, "invalid partition");
		return res;	
	}
	
	if (timeoutms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		return res;	
	}

	RdKafka::ErrorCode error = consumer->query_watermark_offsets(topic, partition, &res.low, &res.hight, timeoutms);
	if (error) {
		res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
		return res;
	}

	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::int64ValueResult KafkaConsumerCore1C::Committed(std::string topic, int32_t partition, int32_t timeoutms)
{

	int64ValueResult res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}
	
	if (topic.empty()){
		res.error = err(ERR_BADPARAMETR, "empty topic");
		return res;	
	}
	
	if (partition < 0){
		res.error = err(ERR_BADPARAMETR, "invalid partition");
		return res;	
	}
	
	if (timeoutms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		return res;	
	}

	RdKafka::TopicPartition* tp;
	try{
		tp = RdKafka::TopicPartition::create(topic, partition);
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "TopicPartition::create error");
		return res;
	}

	std::vector<RdKafka::TopicPartition*> TopicPatrtitionList;
	TopicPatrtitionList.push_back(tp);

	RdKafka::ErrorCode error = consumer->committed(TopicPatrtitionList, timeoutms);
	if (error) {
		res.error = err(ERR_UNHANDLED, RdKafka::err2str(error));
		goto clear;
	}

	res.value = TopicPatrtitionList[0]->offset();
	res.succes = true;

clear:
	TopicPatrtitionList.clear();
	delete tp;

	return res;
}
//---------------------------------------------------------------------------//
void KafkaConsumerCore1C::ClearCurrentAssignmentsList()
{
	for (unsigned int i = 0; i < TopicPatrtitionCurrentAssignments.size(); ++i) {
		delete TopicPatrtitionCurrentAssignments[i];
	}
	TopicPatrtitionCurrentAssignments.clear();
	//TopicPatrtitionCurrentAssignments.shrink_to_fit();
}
//---------------------------------------------------------------------------//
void KafkaConsumerCore1C::ClearLocalQueue()
{
	for (unsigned int i = 0; i < local_queue.size(); ++i) {
		delete local_queue[i];
	}
	local_queue.clear();
	//local_queue.shrink_to_fit();
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaConsumerCore1C::SetLogFilePath(std::string fileName)
{
	RetValue res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		return res;
	}
	
	if (fileName.empty()){
		res.error = err(ERR_BADPARAMETR, "file name is empty");
		return res;
	}

	log_path = fileName;
	res.succes = true;
	return res;

}
//---------------------------------------------------------------------------//
