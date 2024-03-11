#ifndef __COMPONENT_TYPES_H__
#define __COMPONENT_TYPES_H__

#include <stdint.h>
#include <string>
#include <vector>
#include <locale.h>
#include <wchar.h>
#include <stdexcept>
#include <set>
#include "rdkafkacpp.h"
#include "rdkafka.h"

//Errors defenition
#define ERR_SUCCESS 0
#define ERR_EMPTY 1
#define ERR_NOTINIT 2
#define ERR_BADPARAMETR 3
#define ERR_NOTFOUND 4
#define ERR_EOF 5
#define ERR_SIZELIMIT 6
#define ERR_BADALLOC 7
#define ERR_JSONPARSING 8
#define ERR_JSONGENERATION 9
#define ERR_OFFSETSORDER 10
#define ERR_CONSUMPTION 11
#define ERR_TIMEOUT 12
#define ERR_UNHANDLED 100

//Consumer
#define CONSUMER_FATAL_ERROR -1
#define CONSUMER_ERROR 0
#define CONSUMER_NO_ERROR 1

#define MAX_MESSAGES_IN_LOCAL_QUEUE 1000000

//ALL
#define VECTOR_SIZELIMIT 300000


namespace KafkaExport
{
	//error
	struct ErrorDescription
	{
		int32_t type = ERR_SUCCESS;
		bool fatal = false;
		std::string description;
	};
	
	ErrorDescription err(int32_t type, std::string description = "", bool fatal = false);
	std::string err_to_str(ErrorDescription err);
	
	
	//Types
	struct delivery_record {
		std::string key;
		std::string status;
		std::string topic;
		int32_t partition = -1;;
		int64_t timestamp = -1;
		int64_t offset = -1;
		std::string error;
	};
	
	struct WatermarkOffsets {
		std::string topic;
		unsigned int partition = -1;
		int64_t low = -1;
		int64_t hight = -1;
	};

	struct TopicPartitionDescription {
		std::string topic;
		unsigned int partition = -1;
		int64_t offset = -1;
		std::string error;
	};
	
	struct GroupDescription {
		std::string group_id;
		std::string state;
		int32_t is_simple;
	};
	
	struct MetadataBrokerDescription {
		int32_t id;
		std::string host;
		int32_t port;
		bool controller;
	};
	
	struct MetadataPartitionDescription {
		int32_t id;
		int32_t leader;
		std::vector<int32_t> replicas;
		std::vector<int32_t> isrs;
	};
	
	struct MetadataTopicDescription {
		std::string topic;
		std::vector<MetadataPartitionDescription> partitions;
	};
	
	struct MetadataDescription {
		std::vector<MetadataBrokerDescription> brokers;
		std::vector<MetadataTopicDescription> topics;
	};
	
	//Return values
	struct RetValue
	{
		bool succes = false;
		ErrorDescription error;
	};
	
	struct BoolValueResult : RetValue
	{
		bool value;
	};

	struct StringValueResult : RetValue {

		std::string value;
	};

	struct int64ValueResult : RetValue {
		int64_t value;
	};

	struct StringKeyValueResult : RetValue {

		std::string key;
		std::string value;
	};

	struct RetOffsets : RetValue
	{
		int64_t low = -1;
		int64_t hight = -1;
	};

	struct TopicPartitionListResult : RetValue
	{
		std::vector<TopicPartitionDescription> TopicPartitionList;
	};
	
	struct GroupListResult : RetValue
	{
		std::vector<GroupDescription> GroupList;
	};
	
	struct WatermarkOffsetsListResult : RetValue
	{
		std::vector<WatermarkOffsets> WatermarkOffsetsList;
	};
	
	struct MetadataResult : RetValue
	{
		MetadataDescription Metadata;
	};
	
}

#endif 
