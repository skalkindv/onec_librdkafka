#include "stdafx.h"
#include "admin_client1c.h"

using namespace KafkaExport::KafkaAdminClient1C;
//---------------------------------------------------------------------------//
KafkaAdminClientCore::KafkaAdminClientCore()
{
	rk = nullptr;
	Init = false;

	try {
		conf = rd_kafka_conf_new();
	}
	catch (...) {
		conf = nullptr;
	}
}
//---------------------------------------------------------------------------//
KafkaAdminClientCore::~KafkaAdminClientCore()
{
	if (conf != nullptr) {
		rd_kafka_conf_destroy(conf);
		conf = nullptr;
	}
	
	if ((rk_conf != nullptr) && (rk == nullptr)) {
		rd_kafka_conf_destroy(rk_conf);
		rk_conf = nullptr;
	}

	if (rk != nullptr) {
		rd_kafka_destroy(rk);
		rk = nullptr;
	}
}
//---------------------------------------------------------------------------//
bool KafkaAdminClientCore::IsInit()
{
	return Init;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaAdminClientCore::Initialize(std::string _brokers)
{
	RetValue res;
	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf not initialized");
		return res;
	}

	errstr.clear();
	Init = false;
	
	if ((rk_conf != nullptr) && (rk == nullptr)) {
		rd_kafka_conf_destroy(rk_conf);
		rk_conf = nullptr;
	}
	
	if (rk != nullptr) {
		rd_kafka_destroy(rk);
		rk_conf = nullptr;
		rk = nullptr;
	}

	if (_brokers.empty()) {
		res.error = err(ERR_UNHANDLED, "brokers addresses is empty");
		return res;
	}

	brokers = _brokers;
	res = GlobalConfDefaultInit(brokers);
	if (!res.succes) {
		return res;
	}
	
	try{
		rk_conf = rd_kafka_conf_dup(conf);
	}
	catch (...) {
		res.error = err(ERR_UNHANDLED, "rd_kafka_conf_dup error");
		return res;
	}
	
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, rk_conf, errbuf, sizeof(errbuf));
	if (!rk) {
		res.error = err(ERR_UNHANDLED, errbuf);
		return res;
	}

	Init = true;
	res.succes = Init;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaAdminClientCore::ConfReset()
{
	RetValue res;
	if (conf != nullptr) {
		rd_kafka_conf_destroy(conf);
		conf = nullptr;
	}
	
	try {
		conf = rd_kafka_conf_new();
	}
	catch (...) {
		conf = nullptr;
		res.error = err(ERR_UNHANDLED, "rd_kafka_conf_new error");
		return res;
	}

	res = GlobalConfDefaultInit(brokers);
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaAdminClientCore::GlobalConfDefaultInit(std::string _brokers)
{
	RetValue res;
	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf not initialized");
		return res;
	}

	if (!_brokers.empty()) {	
		if (rd_kafka_conf_set(conf, "bootstrap.servers", _brokers.c_str(), errbuf,
		                     	sizeof(errbuf)) != RD_KAFKA_CONF_OK) {
			res.error = err(ERR_UNHANDLED, errbuf);
		}     
	}
	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}

//---------------------------------------------------------------------------//
KafkaExport::RetValue KafkaAdminClientCore::SetGlobalConf(std::string key, std::string value)
{
	RetValue res;
	if (conf == nullptr) {
		res.error = err(ERR_UNHANDLED, "conf not initialized");
		return res;
	}
	
	if (key.empty()) {
		res.error = err(ERR_BADPARAMETR, "key is empty");
		return res;
	}
	
	if (value.empty()) {
		res.error = err(ERR_BADPARAMETR, "value is empty");
		return res;
	}
	
	if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errbuf,
		                  sizeof(errbuf)) != RD_KAFKA_CONF_OK) {
		    res.error = err(ERR_UNHANDLED, errbuf);
		    return res;
	}

	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::TopicPartitionListResult KafkaAdminClientCore::DeleteRecordsBefore(
	std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, int32_t timeout_ms)
{
	rd_kafka_topic_partition_list_t *offsets_before = nullptr; 
 	rd_kafka_DeleteRecords_t *del_records = nullptr;
	rd_kafka_AdminOptions_t *options = nullptr;
	rd_kafka_event_t *event = nullptr;
	rd_kafka_queue_t *queue = nullptr;
	rd_kafka_topic_partition_t *tp = nullptr;		
	const rd_kafka_DeleteRecords_result_t *result = nullptr;
	const rd_kafka_topic_partition_list_t *offsets = nullptr;
	TopicPartitionDescription *ktp = nullptr;
	
	TopicPartitionListResult res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout_ms");
		goto cleanup;
	}
	
	if (TopicPartitionList->size() == 0){
		res.error = err(ERR_BADPARAMETR, "topic-partition list is empty");
		goto cleanup;
	}
	
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		if (ktp->topic.empty()){
			res.error = err(ERR_BADPARAMETR, "empty topic in topic-partition list");
			goto cleanup;
		}
		if (ktp->partition < 0){
			res.error = err(ERR_BADPARAMETR, "invalid partition in topic-partition list");
			goto cleanup;
		}
	}
	
	offsets_before = rd_kafka_topic_partition_list_new((int32_t)TopicPartitionList->size());
	if (!offsets_before){
		res.error = err(ERR_UNHANDLED, "rd_kafka_topic_partition_list_new error");
		goto cleanup;
	}
	
	queue = rd_kafka_queue_new(rk);
	if (!queue){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_new error");
		goto cleanup;
	}
	
	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_DELETERECORDS);
	if (!options){
		res.error = err(ERR_UNHANDLED, "rd_kafka_AdminOptions_new error");
		goto cleanup;
	}
	

	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		tp = rd_kafka_topic_partition_list_add(offsets_before, ktp->topic.c_str(), ktp->partition);
		if (!tp){
			res.error = err(ERR_UNHANDLED, "rd_kafka_topic_partition_list_add error");
			goto cleanup;	
		}
		tp->offset = ktp->offset;
	}
	
	del_records = rd_kafka_DeleteRecords_new(offsets_before);
	if (!del_records){
		res.error = err(ERR_UNHANDLED, "rd_kafka_DeleteRecords_new error");
		goto cleanup;
	}
	
	try{
		rd_kafka_DeleteRecords(rk, &del_records, 1, options, queue);
	}
	catch (...) {
		res.error = err(ERR_UNHANDLED, "rd_kafka_DeleteRecords error");
		goto cleanup;
	}
	
	event = rd_kafka_queue_poll(queue, timeout_ms);
	if (!event){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_poll error");
		goto cleanup;	
	}
	
	if (rd_kafka_event_error(event)){
		res.error = err(ERR_UNHANDLED, rd_kafka_event_error_string(event));
		goto cleanup;
	}
	
	result = rd_kafka_event_DeleteRecords_result(event);
	if (!result){
		res.error = err(ERR_UNHANDLED, "rd_kafka_event_DeleteRecords_result error");
		goto cleanup;	
	}
	
	offsets = rd_kafka_DeleteRecords_result_offsets(result);
	if (!offsets){
		res.error = err(ERR_UNHANDLED, "rd_kafka_DeleteRecords_result_offsets error");
		goto cleanup;	
	}
	
	try {
		for (int32_t i = 0; i < offsets->cnt; i++){	
			
			TopicPartitionDescription TopicPartition;
			TopicPartition.topic = offsets->elems[i].topic;
			TopicPartition.partition = offsets->elems[i].partition;
			TopicPartition.offset = offsets->elems[i].offset;
			TopicPartition.error = rd_kafka_err2str(offsets->elems[i].err);
			
			res.TopicPartitionList.push_back(TopicPartition);
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "TopicPartitionList.push_back error");
		goto cleanup;
	}

	cleanup:	
	
	if (del_records){
		rd_kafka_DeleteRecords_destroy(del_records);
	}
	if (options){
		rd_kafka_AdminOptions_destroy(options);
	}
	if (event){
		rd_kafka_event_destroy(event);
	}
	if (queue){
		rd_kafka_queue_destroy(queue);
	}
	if (offsets_before){
		rd_kafka_topic_partition_list_destroy(offsets_before);
	}
		
	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::TopicPartitionListResult KafkaAdminClientCore::GetGroupOffsets(
	std::string group_id, int32_t timeout_ms)
{
 	rd_kafka_AdminOptions_t *options = nullptr;
 	rd_kafka_queue_t *queue = nullptr;
	rd_kafka_event_t *event = nullptr;
	rd_kafka_ListConsumerGroupOffsets_t *list_cgrp_offsets = nullptr;
	rd_kafka_error_t *error = nullptr;
	
	const rd_kafka_ListConsumerGroupOffsets_result_t *result = nullptr;
	const rd_kafka_group_result_t **groups = nullptr;
	const rd_kafka_group_result_t *group = nullptr;
	const rd_kafka_topic_partition_list_t *partitions = nullptr;	
	size_t n_groups;

	TopicPartitionListResult res;		
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (group_id.empty()){
		res.error = err(ERR_BADPARAMETR, "group id is empty");
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		goto cleanup;
	}
	
	queue = rd_kafka_queue_new(rk);
	if (!queue){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_new error");
		goto cleanup;
	}
	
	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS);
	if (!options){
		res.error = err(ERR_UNHANDLED, "rd_kafka_AdminOptions_new error");
		goto cleanup;
	}
		
	
	if ((error = rd_kafka_AdminOptions_set_require_stable_offsets
		(options, 0))) {
		res.error = err(ERR_UNHANDLED, rd_kafka_error_string(error));
		goto cleanup;
	}
	
	list_cgrp_offsets = rd_kafka_ListConsumerGroupOffsets_new(group_id.c_str(), nullptr);
	if (!list_cgrp_offsets){
		res.error = err(ERR_UNHANDLED, "rd_kafka_ListConsumerGroupOffsets_new error");
		goto cleanup;
	}
	
	
	try{
		rd_kafka_ListConsumerGroupOffsets(rk, &list_cgrp_offsets, 1, options, queue);
	}
	catch (...) {
		res.error = err(ERR_UNHANDLED, "rd_kafka_ListConsumerGroupOffsets error");
		goto cleanup;
	}
		
	
	event = rd_kafka_queue_poll(queue, timeout_ms);
	if (!event){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_poll error");
		goto cleanup;	
	}
	
	
	if (rd_kafka_event_error(event)){
		res.error = err(ERR_UNHANDLED, rd_kafka_event_error_string(event));
		goto cleanup;
	}
	
	result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
	if (!result){
		res.error = err(ERR_UNHANDLED, "rd_kafka_event_ListConsumerGroupOffsets_result error");
		goto cleanup;	
	}
	
	groups = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &n_groups);
	if (!groups){
		res.error = err(ERR_UNHANDLED, "rd_kafka_ListConsumerGroupOffsets_result_groups");
		goto cleanup;	
	}
	
	for (size_t grp = 0; grp < n_groups; grp++) {
		group = groups[grp];
		partitions = rd_kafka_group_result_partitions(group);
		         
		try {   
			for (int32_t i = 0; i < partitions->cnt; i++) {		
				TopicPartitionDescription TopicPartition;
				TopicPartition.topic = partitions->elems[i].topic;
				TopicPartition.partition = partitions->elems[i].partition;
				TopicPartition.offset = partitions->elems[i].offset;
				TopicPartition.error = rd_kafka_err2str(partitions->elems[i].err);
				
				res.TopicPartitionList.push_back(TopicPartition);
			}	
		}	 
		catch (...) {
			res.error = err(ERR_BADALLOC, "TopicPartitionList.push_back error");
			goto cleanup;
		}   
	}
	
	cleanup:	
	
	if (error){
		 rd_kafka_error_destroy(error);
	}
	
	if (list_cgrp_offsets){
		 rd_kafka_ListConsumerGroupOffsets_destroy(list_cgrp_offsets);
	}
	if (options){
		rd_kafka_AdminOptions_destroy(options);
	}
	if (event){
		rd_kafka_event_destroy(event);
	}
	if (queue){
		rd_kafka_queue_destroy(queue);
	}
	
	res.succes = res.error.type == ERR_SUCCESS;
	return res;

}
//---------------------------------------------------------------------------//
KafkaExport::GroupListResult KafkaAdminClientCore::GetGroupList(int32_t timeout_ms)
{
	rd_kafka_AdminOptions_t *options = nullptr;
	rd_kafka_event_t *event = nullptr;
	rd_kafka_queue_t *queue = nullptr;
	
	const rd_kafka_ListConsumerGroups_result_t *result = nullptr;	
	const rd_kafka_ConsumerGroupListing_t **result_groups = nullptr;
	size_t result_groups_cnt;
	const rd_kafka_ConsumerGroupListing_t *group = nullptr;
	const rd_kafka_error_t **errors = nullptr;
	size_t result_error_cnt;
	const rd_kafka_error_t *error = nullptr;
	std::string group_errors;
	
	GroupListResult res;		
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		goto cleanup;
	}	
	
	queue = rd_kafka_queue_new(rk);
	if (!queue){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_new error");
		goto cleanup;
	}
	
	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS);
	if (!options){
		res.error = err(ERR_UNHANDLED, "rd_kafka_AdminOptions_new error");
		goto cleanup;
	}
    
	try{
		rd_kafka_ListConsumerGroups(rk, options, queue);
	}
	catch (...) {
		res.error = err(ERR_UNHANDLED, "rd_kafka_ListConsumerGroups error");
		goto cleanup;
	}
	
	event = rd_kafka_queue_poll(queue, timeout_ms);
	if (!event){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_poll");
		goto cleanup;	
	}
	
	if (rd_kafka_event_error(event)){
		res.error = err(ERR_UNHANDLED, rd_kafka_event_error_string(event));
		goto cleanup;
	}
	
	result = rd_kafka_event_ListConsumerGroups_result(event);
	if (!result){
		res.error = err(ERR_UNHANDLED, "rd_kafka_event_ListConsumerGroups_result error");
		goto cleanup;	
	}
	
	errors = rd_kafka_ListConsumerGroups_result_errors(result, &result_error_cnt);
	for (size_t i = 0; i < result_error_cnt; i++) {
		error = errors[i];        
		group_errors = group_errors + (group_errors.empty() ? "" : "\r\n") 
			+ "Error[" + std::to_string(rd_kafka_error_code(error)) + "] " + rd_kafka_error_string(error);
	}
	
	result_groups = rd_kafka_ListConsumerGroups_result_valid(result, &result_groups_cnt);
	try {
		for (size_t i = 0; i < result_groups_cnt; i++) {
			group = result_groups[i];
			rd_kafka_consumer_group_state_t state = rd_kafka_ConsumerGroupListing_state(group);
		
			GroupDescription Group;
			Group.group_id = rd_kafka_ConsumerGroupListing_group_id(group);;
			Group.state = rd_kafka_consumer_group_state_name(state);
			Group.is_simple = rd_kafka_ConsumerGroupListing_is_simple_consumer_group(group);

			res.GroupList.push_back(Group);
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "GroupList.push_back error");
		goto cleanup;
	}
	
	cleanup:	
	
	if (options){
		rd_kafka_AdminOptions_destroy(options);
	}
	if (event){
		rd_kafka_event_destroy(event);
	}
	if (queue){
		rd_kafka_queue_destroy(queue);
	}
	
	res.succes = res.error.type == ERR_SUCCESS;
	if (res.succes){
		res.error = err(ERR_SUCCESS, group_errors);
	}
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::MetadataResult KafkaAdminClientCore::GetMetadata(int32_t timeout_ms, std::string topic)
{
	rd_kafka_resp_err_t error;
	int32_t controllerid;
	rd_kafka_topic_t *rkt = nullptr;
	const struct rd_kafka_metadata *metadata = nullptr;
	const struct rd_kafka_metadata_topic *t = nullptr;
	const struct rd_kafka_metadata_partition *p = nullptr;
	
	MetadataResult res;		
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		goto cleanup;
	}
	
	if (!topic.empty()){
		rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);
	}
	
	error = rd_kafka_metadata(rk, rkt ? 0 : 1, rkt, &metadata, timeout_ms);
	if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
		res.error = err(ERR_UNHANDLED, rd_kafka_err2str(error));
		goto cleanup;
	}
    
    	controllerid = rd_kafka_controllerid(rk, timeout_ms);
    	
    	try {
		for (int32_t i = 0; i < metadata->broker_cnt; i++){
			MetadataBrokerDescription broker_desc;	
			broker_desc.id = metadata->brokers[i].id;
			broker_desc.host = metadata->brokers[i].host;
			broker_desc.port  = metadata->brokers[i].port;
			broker_desc.controller = (controllerid == metadata->brokers[i].id);
			res.Metadata.brokers.push_back(broker_desc);
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "Metadata.brokers.push_back error");
		goto cleanup;
	}
	
	try {
		for (int32_t i = 0; i < metadata->topic_cnt; i++) {
		
			t = &metadata->topics[i];
			if (t->err) {
				res.error = err(ERR_UNHANDLED, rd_kafka_err2str(t->err));
				goto cleanup;
			}
			
			MetadataTopicDescription topic_desc;
			topic_desc.topic = t->topic;	
			for (int32_t j = 0; j < t->partition_cnt; j++) {
				p = &t->partitions[j];
				if (p->err){
					res.error = err(ERR_UNHANDLED, rd_kafka_err2str(p->err));
					goto cleanup;
				}		
				MetadataPartitionDescription partition_desc;
				partition_desc.id = p->id;
				partition_desc.leader = p->leader;

				for (int32_t k = 0; k < p->replica_cnt; k++)
					partition_desc.replicas.push_back(p->replicas[k]);
				for (int32_t k = 0; k < p->isr_cnt; k++)
					partition_desc.isrs.push_back(p->isrs[k]);	
						
				topic_desc.partitions.push_back(partition_desc);	         
			}
			res.Metadata.topics.push_back(topic_desc);
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "Metadata.topics.push_back error");
		goto cleanup;
	}

	cleanup:

	if (metadata){
		rd_kafka_metadata_destroy(metadata);
	}
	
	if (rkt){
		rd_kafka_topic_destroy(rkt);
	}
                        	
	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::TopicPartitionListResult KafkaAdminClientCore::AlterGroupOffsets(
	std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, std::string group_id, int32_t timeout_ms)
{
 	rd_kafka_AdminOptions_t *options = nullptr;
 	rd_kafka_topic_partition_list_t *offsets = nullptr;
 	rd_kafka_AlterConsumerGroupOffsets_t *alter_consumer_group_offsets = nullptr;
 	rd_kafka_queue_t *queue = nullptr;
 	rd_kafka_event_t *event = nullptr;
 	rd_kafka_topic_partition_t *tp = nullptr;
	const rd_kafka_AlterConsumerGroupOffsets_result_t *result = nullptr;
	const rd_kafka_group_result_t **groups = nullptr;
	const rd_kafka_group_result_t *group = nullptr;
	const rd_kafka_topic_partition_list_t *partitions = nullptr;
	TopicPartitionDescription *ktp = nullptr;
 	size_t n_groups;
	
	TopicPartitionListResult res;	
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		goto cleanup;
	}
	
	if (group_id.empty()){
		res.error = err(ERR_BADPARAMETR, "group id is empty");
		goto cleanup;
	}
	
	if (TopicPartitionList->size() == 0){
		res.error = err(ERR_BADPARAMETR, "topic-partition list is empty");
		goto cleanup;
	}
	
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		if (ktp->topic.empty()){
			res.error = err(ERR_BADPARAMETR, "empty topic in topic-partition list");
			goto cleanup;
		}
		if (ktp->partition < 0){
			res.error = err(ERR_BADPARAMETR, "invalid partition in topic-partition list");
			goto cleanup;
		}
	}
	
	queue = rd_kafka_queue_new(rk);
	if (!queue){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_new error");
		goto cleanup;
	}
	
	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);
	if (!options){
		res.error= err(ERR_UNHANDLED, "rd_kafka_AdminOptions_new error");
		goto cleanup;
	}
	
    
	offsets = rd_kafka_topic_partition_list_new((int32_t)TopicPartitionList->size());
	if (!offsets){
		res.error = err(ERR_UNHANDLED, "rd_kafka_topic_partition_list_new error");
		goto cleanup;
	}
	
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		tp = rd_kafka_topic_partition_list_add(offsets, ktp->topic.c_str(), ktp->partition);
		if (!tp){
			res.error = err(ERR_UNHANDLED, "rd_kafka_topic_partition_list_add error");
			goto cleanup;	
		}
		tp->offset = ktp->offset;
	}
	
	alter_consumer_group_offsets = rd_kafka_AlterConsumerGroupOffsets_new(group_id.c_str(), offsets);
	if (!alter_consumer_group_offsets){
		res.error = err(ERR_UNHANDLED, "rd_kafka_AlterConsumerGroupOffsets_new error");
		goto cleanup;	
	}

	try{
		rd_kafka_AlterConsumerGroupOffsets(rk, &alter_consumer_group_offsets, 1, options, queue);
	}	
	catch (...) {
		res.error = err(ERR_UNHANDLED, "rd_kafka_AlterConsumerGroupOffsets error");
		goto cleanup;
	}
	
	event = rd_kafka_queue_poll(queue, timeout_ms);
	if (!event){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_poll error");
		goto cleanup;	
	}
	
	if (rd_kafka_event_error(event)){
		res.error = err(ERR_UNHANDLED, rd_kafka_event_error_string(event));
		goto cleanup;
	}
	
	result = rd_kafka_event_AlterConsumerGroupOffsets_result(event);
	if (!result){
		res.error = err(ERR_UNHANDLED, "rd_kafka_event_AlterConsumerGroupOffsets_result error");
		goto cleanup;	
	}
                
	groups = rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &n_groups);
	if (!groups){
		res.error = err(ERR_UNHANDLED, "rd_kafka_AlterConsumerGroupOffsets_result_groups error");
		goto cleanup;	
	}
                    
	try{	
		for (size_t grp = 0; grp < n_groups; grp++) {
			group = groups[grp];
			partitions = rd_kafka_group_result_partitions(group);
				    
			for (int32_t i = 0; i < partitions->cnt; i++) {
				TopicPartitionDescription TopicPartition;
				TopicPartition.topic = partitions->elems[i].topic;
				TopicPartition.partition = partitions->elems[i].partition;
				TopicPartition.offset = partitions->elems[i].offset;
				TopicPartition.error = rd_kafka_err2str(partitions->elems[i].err);
				
				res.TopicPartitionList.push_back(TopicPartition);
			}	    
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "TopicPartitionList.push_back error");
		goto cleanup;
	}
	
	cleanup:	
	
	if (alter_consumer_group_offsets){
		rd_kafka_AlterConsumerGroupOffsets_destroy(
			alter_consumer_group_offsets);
	}	
	if (options){
		rd_kafka_AdminOptions_destroy(options);
	}
	if (offsets){
		rd_kafka_topic_partition_list_destroy(offsets);
	}
	if (event){
		rd_kafka_event_destroy(event);
	}
	if (queue){
		rd_kafka_queue_destroy(queue);
	}
		
	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::TopicPartitionListResult KafkaAdminClientCore::DeleteGroupOffsets(
	std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, std::string group_id, int32_t timeout_ms)
{
 	rd_kafka_AdminOptions_t *options = nullptr;
 	rd_kafka_topic_partition_list_t *offsets = nullptr;
 	rd_kafka_DeleteConsumerGroupOffsets_t *delete_consumer_group_offsets = nullptr;
 	rd_kafka_queue_t *queue = nullptr;
 	rd_kafka_event_t *event = nullptr;
 	rd_kafka_topic_partition_t *tp = nullptr;
	const rd_kafka_DeleteConsumerGroupOffsets_result_t *result = nullptr;
	const rd_kafka_group_result_t **groups = nullptr;
	const rd_kafka_group_result_t *group = nullptr;
	const rd_kafka_topic_partition_list_t *partitions = nullptr;
	TopicPartitionDescription *ktp = nullptr;
 	size_t n_groups;
	
	TopicPartitionListResult res;		
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		goto cleanup;
	}
	
	if (group_id.empty()){
		res.error = err(ERR_BADPARAMETR, "group id is empty");
		goto cleanup;
	}
	
	if (TopicPartitionList->size() == 0){
		res.error = err(ERR_BADPARAMETR, "topic-partition list is empty");
		goto cleanup;
	}
	
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		if (ktp->topic.empty()){
			res.error = err(ERR_BADPARAMETR, "empty topic in topic-partition list");
			goto cleanup;
		}
		if (ktp->partition < 0){
			res.error = err(ERR_BADPARAMETR, "invalid partition in topic-partition list");
			goto cleanup;
		}
	}
	
	queue = rd_kafka_queue_new(rk);
	if (!queue){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_new error");
		goto cleanup;
	}
	
	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS);
	if (!options){
		res.error = err(ERR_UNHANDLED, "rd_kafka_AdminOptions_new error");
		goto cleanup;
	}
	
    
    	offsets = rd_kafka_topic_partition_list_new((int32_t)TopicPartitionList->size());
	if (!offsets){
		res.error = err(ERR_UNHANDLED, "rd_kafka_topic_partition_list_new error");
		goto cleanup;
	}
	
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		tp = rd_kafka_topic_partition_list_add(offsets, ktp->topic.c_str(), ktp->partition);
		if (!tp){
			res.error = err(ERR_UNHANDLED, "rd_kafka_topic_partition_list_add error");
			goto cleanup;	
		}
		tp->offset = ktp->offset;
	}
	
	delete_consumer_group_offsets = rd_kafka_DeleteConsumerGroupOffsets_new(group_id.c_str(), offsets);
	if (!delete_consumer_group_offsets){
		res.error = err(ERR_UNHANDLED, "rd_kafka_DeleteConsumerGroupOffsets_new error");
		goto cleanup;	
	}

	try{
		rd_kafka_DeleteConsumerGroupOffsets(rk, &delete_consumer_group_offsets, 1, options, queue);
	}	
	catch (...) {
		res.error = err(ERR_UNHANDLED, "rd_kafka_DeleteConsumerGroupOffsets error");
		goto cleanup;
	}
	
	event = rd_kafka_queue_poll(queue, timeout_ms);
	if (!event){
		res.error = err(ERR_UNHANDLED, "rd_kafka_queue_poll error");
		goto cleanup;	
	}
	
	if (rd_kafka_event_error(event)){
		res.error = err(ERR_UNHANDLED, rd_kafka_event_error_string(event));
		goto cleanup;
	}
	
	result = rd_kafka_event_DeleteConsumerGroupOffsets_result(event);
	if (!result){
		res.error = err(ERR_UNHANDLED, "rd_kafka_event_DeleteConsumerGroupOffsets_result error");
		goto cleanup;	
	}
                
	groups = rd_kafka_DeleteConsumerGroupOffsets_result_groups(result, &n_groups);
	if (!groups){
		res.error = err(ERR_UNHANDLED, "rd_kafka_DeleteConsumerGroupOffsets_result_groups error");
		goto cleanup;	
	}
            
	try {        	
		for (size_t grp = 0; grp < n_groups; grp++) {
			group = groups[grp];
			partitions = rd_kafka_group_result_partitions(group);
				    
			for (int32_t i = 0; i < partitions->cnt; i++) {
				TopicPartitionDescription TopicPartition;
				TopicPartition.topic = partitions->elems[i].topic;
				TopicPartition.partition = partitions->elems[i].partition;
				TopicPartition.offset = partitions->elems[i].offset;
				TopicPartition.error = rd_kafka_err2str(partitions->elems[i].err);
				
				res.TopicPartitionList.push_back(TopicPartition);
			}	    
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "TopicPartitionList.push_back error");
		goto cleanup;
	}
	
	cleanup:	
	
	if (delete_consumer_group_offsets){
		rd_kafka_DeleteConsumerGroupOffsets_destroy(
			delete_consumer_group_offsets);
	}	
	if (options){
		rd_kafka_AdminOptions_destroy(options);
	}
	if (offsets){
		rd_kafka_topic_partition_list_destroy(offsets);
	}
	if (event){
		rd_kafka_event_destroy(event);
	}
	if (queue){
		rd_kafka_queue_destroy(queue);
	}
		
	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
KafkaExport::WatermarkOffsetsListResult KafkaAdminClientCore::QueryWatermarkOffsets(
	std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, int32_t timeout_ms)
{
	rd_kafka_resp_err_t error;
	TopicPartitionDescription *ktp = nullptr;
	
	WatermarkOffsetsListResult res;
	if (!IsInit()) {
		res.error = err(ERR_NOTINIT);
		goto cleanup;
	}
	
	if (timeout_ms <= 0){
		res.error = err(ERR_BADPARAMETR, "invalid timeout");
		goto cleanup;
	}
	
	if (TopicPartitionList->size() == 0){
		res.error = err(ERR_BADPARAMETR, "topic-partition list is empty");
		goto cleanup;
	}
	
	for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
		ktp = &TopicPartitionList->at(i);
		if (ktp->topic.empty()){
			res.error = err(ERR_BADPARAMETR, "empty topic in topic-partition list");
			goto cleanup;
		}
		if (ktp->partition < 0){
			res.error = err(ERR_BADPARAMETR, "invalid partition in topic-partition list");
			goto cleanup;
		}
	}
	
	try {
		for (unsigned int i = 0; i < TopicPartitionList->size(); ++i) {
			ktp = &TopicPartitionList->at(i);
					
			WatermarkOffsets offsets;
			offsets.topic = ktp->topic;
			offsets.partition = ktp->partition;
			error = rd_kafka_query_watermark_offsets(rk, ktp->topic.c_str(), ktp->partition, &offsets.low, &offsets.hight, timeout_ms);
			if (error != RD_KAFKA_RESP_ERR_NO_ERROR) {
				res.error = err(ERR_UNHANDLED, rd_kafka_err2str(error));
				goto cleanup;
			}
			res.WatermarkOffsetsList.push_back(offsets);
		}
	}
	catch (...) {
		res.error = err(ERR_BADALLOC, "WatermarkOffsetsList.push_back error");
		goto cleanup;
	}

	cleanup:
	res.succes = res.error.type == ERR_SUCCESS;
	return res;
}
//---------------------------------------------------------------------------//
