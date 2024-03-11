#include "stdafx.h"
#include "data_conversion.h"

using namespace KafkaExport::DataConversion;

//---------------------------------------------------------------------------//
DataBuilder::DataBuilder()
{
}
//---------------------------------------------------------------------------//
DataBuilder::~DataBuilder()
{
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue DataBuilder::AppendHeaderFilter(std::string heder_key)
{
	KafkaExport::RetValue Res;
	if (heder_key.empty()) {
		Res.error = err(ERR_BADPARAMETR, "header key is empty");
		return Res;
	}

	if (HeaderFilter.size() >= VECTOR_SIZELIMIT) {
		Res.error = err(ERR_SIZELIMIT, "header filter list is full");
		return Res;
	}

	try {
		HeaderFilter.insert(heder_key);
	}
	catch (...) {
		Res.error = err(ERR_BADALLOC, "HeaderFilter.insert error");
		return Res;
	}
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue DataBuilder::ClearHeaderFilter()
{
	KafkaExport::RetValue Res;
	HeaderFilter.clear();
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_DeliveryReport(std::vector<KafkaExport::delivery_record> records, bool DecodeBase64Key)
{
	KafkaExport::StringValueResult Res;
	std::string key;
	try {
		auto jsonObjects = nlohmann::ordered_json::array();
		for (unsigned int i = 0; i < records.size(); ++i) {

			auto record = nlohmann::ordered_json::object();

			key.clear();
			if (!DecodeBase64Key) {
				key.append(records[i].key);
			}
			else {
				base64_encode_((const unsigned char*)records[i].key.c_str(), (unsigned int)records[i].key.size(), &key);
			}

			record["Key"] = key;		
			record["Topic"] = records[i].topic;
			record["Partition"] = records[i].partition;
			record["Timestamp"] = records[i].timestamp;
			record["Offset"] = records[i].offset;
			record["Error"] = records[i].error;
			record["Status"] = records[i].status;

			jsonObjects.push_back(record);
		}

		Res.value = jsonObjects.dump(4);
	}
	catch (...) {
		Res.error = err(ERR_JSONGENERATION, "delivery report generation error");
		return Res;
	}
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_TopicPartitionList(std::vector<KafkaExport::TopicPartitionDescription>* records)
{
	KafkaExport::StringValueResult Res;
	try {
		auto jsonObjects = nlohmann::ordered_json::array();
		for (unsigned int i = 0; i < records->size(); ++i) {

			auto record = nlohmann::ordered_json::object();
			record["Topic"] = records->at(i).topic;
			record["Partition"] = records->at(i).partition;
			record["Offset"] = records->at(i).offset;
			record["Error"] = records->at(i).error;

			jsonObjects.push_back(record);
		}

		Res.value = jsonObjects.dump(4);
	}
	catch (...) {
		Res.error = err(ERR_JSONGENERATION, "topic-partition list generation error");
		return Res;
	}
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_WatermarkOffsets(std::vector<KafkaExport::WatermarkOffsets>* records)
{
	KafkaExport::StringValueResult Res;
	try {
		auto jsonObjects = nlohmann::ordered_json::array();
		for (unsigned int i = 0; i < records->size(); ++i) {

			auto record = nlohmann::ordered_json::object();
			record["Topic"] = records->at(i).topic;
			record["Partition"] = records->at(i).partition;
			record["Low"] = records->at(i).low;
			record["Hight"] = records->at(i).hight;

			jsonObjects.push_back(record);
		}

		Res.value = jsonObjects.dump(4);
	}
	catch (...) {
		Res.error = err(ERR_JSONGENERATION, "watermark offsets list generation error");
		return Res;
	}
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_GroupList(std::vector<KafkaExport::GroupDescription>* records)
{
	KafkaExport::StringValueResult Res;
	try {
		auto jsonObjects = nlohmann::ordered_json::array();
		for (unsigned int i = 0; i < records->size(); ++i) {

			auto record = nlohmann::ordered_json::object();
			record["GroupId"] = records->at(i).group_id;
			record["State"] = records->at(i).state;
			record["IsSimple"] = records->at(i).is_simple;

			jsonObjects.push_back(record);
		}

		Res.value = jsonObjects.dump(4);
	}
	catch (...) {
		Res.error = err(ERR_JSONGENERATION, "group list generation error");
		return Res;
	}
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_Metadata(KafkaExport::MetadataDescription *metadata)
{
	KafkaExport::StringValueResult Res;
	try {
	
		auto main = nlohmann::ordered_json::object();
		main["Brokers"] = nlohmann::ordered_json::array();
		main["Topics"] = nlohmann::ordered_json::array();
		
		for (unsigned int i = 0; i < metadata->brokers.size(); ++i){
			auto broker = nlohmann::ordered_json::object();
			broker["Id"] = metadata->brokers[i].id;
			broker["Host"] = metadata->brokers[i].host;
			broker["Port"] = metadata->brokers[i].port;
			broker["Controller"] = metadata->brokers[i].controller;
			
			main["Brokers"].push_back(broker);
		}
		for (unsigned int i = 0; i < metadata->topics.size(); ++i) {
			auto topic = nlohmann::ordered_json::object();
			topic["Topic"] = metadata->topics[i].topic;
			topic["Partitions"] = nlohmann::ordered_json::array();
			for (unsigned int j = 0; j < metadata->topics[i].partitions.size(); ++j) {
				auto partition = nlohmann::ordered_json::object();
				partition["Id"] = metadata->topics[i].partitions[j].id;
				partition["Leader"] = metadata->topics[i].partitions[j].leader;
				
				partition["Replicas"] = nlohmann::ordered_json::array();			
				for (unsigned int k = 0; k < metadata->topics[i].partitions[j].replicas.size(); ++k) {
					partition["Replicas"].push_back(metadata->topics[i].partitions[j].replicas[k]);
				}		
				partition["Isrs"] = nlohmann::ordered_json::array();
				for (unsigned int k = 0; k < metadata->topics[i].partitions[j].isrs.size(); ++k) {
					partition["Isrs"].push_back(metadata->topics[i].partitions[j].isrs[k]);
				}	
				topic["Partitions"].push_back(partition);
			}
			main["Topics"].push_back(topic);
		}
		Res.value = main.dump(4);
	}
	catch (...) {
		Res.error = err(ERR_JSONGENERATION, "metadata list generation error");
		return Res;
	}
	Res.succes = true;
	return Res;
}
//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_GetJSONWatermarkOffsets(KafkaExport::RetOffsets offsets)
{
	KafkaExport::StringValueResult res;
	try {
		auto record = nlohmann::ordered_json::object();

		record["Low"] = offsets.low;
		record["Hight"] = offsets.hight;

		res.value = record.dump();
	}
	catch (...) {
		res.error = err(ERR_JSONGENERATION, "watermark offsets generation error");
		return res;
	}
	res.succes = true;
	return res;
}

//---------------------------------------------------------------------------//
void DataBuilder::escape_string_simple(std::string* buffer, const char* data, size_t len)
{
	char buff[6];

	for (int c = 0; c != len; c++) {

		switch (data[c]) {

		case '"':
			buffer->append("\\\"");
			break;
		case '\\':
			buffer->append("\\\\");
			break;
		case '\b':
			buffer->append("\\b");
			break;
		case '\f':
			buffer->append("\\f");
			break;
		case '\n':
			buffer->append("\\n");
			break;
		case '\r':
			buffer->append("\\r");
			break;
		case '\t':
			buffer->append("\\t");
			break;
		default:
			if ('\x00' <= data[c] && data[c] <= '\x1f') {

				buffer->append("\\u");
				sprintf(buff, "u%04x", int(data[c]));
				buffer->append(buff);
			}
			else {
				buffer->push_back(data[c]);
			}
		}
	}
}
//---------------------------------------------------------------------------//
void DataBuilder::append_key_value_string(std::string* buffer, const char* key, const char* data, size_t len)
{
	if (len == 0) len = strlen(data);

	buffer->append("\"");
	buffer->append(key);
	buffer->append("\":\"");
	buffer->append(data, len);
	buffer->append("\"");
}
//---------------------------------------------------------------------------//
void DataBuilder::append_key_value_value(std::string* buffer, const char* key, const char* data, size_t len)
{
	if (len == 0) len = strlen(data);

	buffer->append("\"");
	buffer->append(key);
	buffer->append("\":");
	buffer->append(data, len);
}
//---------------------------------------------------------------------------//
void DataBuilder::append_key_value_escaped_string(std::string* buffer, const char* key, const char* data, size_t len)
{
	if (len == 0) len = strlen(data);

	buffer->append("\"");
	buffer->append(key);
	buffer->append("\":\"");
	escape_string_simple(buffer, data, len);
	buffer->append("\"");

}
//---------------------------------------------------------------------------//
void DataBuilder::append_key_value_base64_encoded_string(std::string* buffer, const char* key, const unsigned char* data, size_t len)
{
	if (len == 0) len = strlen((const char*)data);

	buffer->append("\"");
	buffer->append(key);
	buffer->append("\":\"");
	base64_encode_(data, len, buffer);
	buffer->append("\"");
}

//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::JSON_KafkaMessagePool(std::vector<RdKafka::Message*>* local_queue, bool base64encode)
{
	KafkaExport::StringValueResult Res;
	char itoa_buffer[65];
	bool headerfilter = HeaderFilter.size() > 0;

	try {
		Res.value.append("[");
		for (unsigned int i = 0; i < local_queue->size(); ++i) {

			if (i != 0) {
				Res.value.append(",");
			}

			Res.value.append("{");
			//Topic
			append_key_value_string(&Res.value, "Topic", local_queue->at(i)->topic_name().c_str());

			//Partition
			Res.value.append(",");
			sprintf(itoa_buffer, "%d", local_queue->at(i)->partition());
			append_key_value_value(&Res.value, "Partition", itoa_buffer);

			//Timestamp
			Res.value.append(",");
			sprintf(itoa_buffer, "%" PRId64, local_queue->at(i)->timestamp().timestamp);
			append_key_value_value(&Res.value, "Timestamp", itoa_buffer);

			//Offset
			Res.value.append(",");
			sprintf(itoa_buffer, "%" PRId64, local_queue->at(i)->offset());
			append_key_value_value(&Res.value, "Offset", itoa_buffer);

			if (!base64encode) {
				//Key
				Res.value.append(",");
				if (local_queue->at(i)->key()) {
					if (EscapeMessageKey)
						append_key_value_escaped_string(&Res.value, "Key", local_queue->at(i)->key()->c_str(), local_queue->at(i)->key()->size());
					else
						append_key_value_value(&Res.value, "Key", local_queue->at(i)->key()->c_str(), local_queue->at(i)->key()->size());
				}
				else {
					append_key_value_string(&Res.value, "Key", "");
				}

				//Value
				Res.value.append(",");
				if (EscapeMessageValue)
					append_key_value_escaped_string(&Res.value, "Value", (const char*)local_queue->at(i)->payload(), local_queue->at(i)->len());
				else
					append_key_value_value(&Res.value, "Value", (const char*)local_queue->at(i)->payload(), local_queue->at(i)->len());
			}
			else {
				//Key
				Res.value.append(",");
				if (local_queue->at(i)->key()) {
					append_key_value_base64_encoded_string(&Res.value, "Key", (const unsigned char*)local_queue->at(i)->key()->c_str(), (unsigned int)local_queue->at(i)->key()->size());
				}
				else {
					append_key_value_string(&Res.value, "Key", "");
				}

				//Value
				Res.value.append(",");
				append_key_value_base64_encoded_string(&Res.value, "Value", (const unsigned char*)local_queue->at(i)->payload(), (unsigned int)local_queue->at(i)->len());
			}

			//Headers
			Res.value.append(",");
			append_key_value_value(&Res.value, "Headers", "[");

			const RdKafka::Headers* headers = local_queue->at(i)->headers();
			if (headers) {
				int appended_headers = 0;
				std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
				for (size_t HeaderIndex = 0; HeaderIndex < hdrs.size(); ++HeaderIndex) {

					if (headerfilter) {
						if (HeaderFilter.find(hdrs[HeaderIndex].key().c_str()) != HeaderFilter.end()) {
							continue;
						}
					}

					if (appended_headers != 0) {
						Res.value.append(",");
					}

					Res.value.append("{");
					//Key
					if (EscapeMessageHeaderKey)
						append_key_value_escaped_string(&Res.value, "Key", hdrs[HeaderIndex].key().c_str(), hdrs[HeaderIndex].key().size());
					else
						append_key_value_value(&Res.value, "Key", hdrs[HeaderIndex].key().c_str(), hdrs[HeaderIndex].key().size());

					Res.value.append(",");
					if (!base64encode) {
						//Value
						if (EscapeMessageHeaderValue)
							append_key_value_escaped_string(&Res.value, "Value", hdrs[HeaderIndex].value_string(), hdrs[HeaderIndex].value_size());
						else
							append_key_value_value(&Res.value, "Value", hdrs[HeaderIndex].value_string(), hdrs[HeaderIndex].value_size());
					}
					else {
						//Value
						append_key_value_base64_encoded_string(&Res.value, "Value", (const unsigned char*)hdrs[HeaderIndex].value_string(), (unsigned int)hdrs[HeaderIndex].value_size());
					}
					Res.value.append("}");
					appended_headers++;
				}
			}
			Res.value.append("]}");
			if (RemoveMessagesFromLocalQueueOnBuild) delete local_queue->at(i);
		}
		Res.value.append("]");
		if (RemoveMessagesFromLocalQueueOnBuild) local_queue->clear();
	}
	catch (...) {
		Res.error = err(ERR_JSONGENERATION, "message list generation error");
		if (RemoveMessagesFromLocalQueueOnBuild) Res.error.fatal = true;
		return Res;
	}

	Res.succes = true;
	return Res;
}


//---------------------------------------------------------------------------//
void DataBuilder::internal_escape_string_simple(std::string* buffer, const char* data, size_t len)
{
	for (int c = 0; c != len; c++) {

		switch (data[c]) {

		case '"':
			buffer->append("\"\"");
			break;
		default:
			buffer->push_back(data[c]);
		}
	}
}
//---------------------------------------------------------------------------//
void DataBuilder::internal_clmnstr(std::string* buffer, const char* value, size_t len, bool comma)
{
	if (len == 0) len = strlen(value);
	
	if (comma) buffer->append(",");
	buffer->append("{\"S\",\"");
	internal_escape_string_simple(buffer, value, len);
	//buffer->append(value, len);
	buffer->append("\"}");
}
//---------------------------------------------------------------------------//
void DataBuilder::internal_clmnbin(std::string* buffer, const unsigned char* value, size_t len, bool comma)
{
	if (len == 0) len = strlen((const char*)value);
	
	if (comma) buffer->append(",");
	buffer->append("{\"#\",87126200-3e98-44e0-b931-ccb1d7edc497,{1,{#base64:");
	base64_encode_(value, len, buffer);
	buffer->append("}}}");
}
//---------------------------------------------------------------------------//
void DataBuilder::internal_clmnnum(std::string* buffer, const char* value, size_t len)
{
	if (len == 0) len = strlen(value);
	
	buffer->append(",{\"N\",");
	buffer->append(value, len);
	buffer->append("}");
}

//---------------------------------------------------------------------------//
KafkaExport::StringValueResult DataBuilder::OnecInternal_KafkaMessagePool(std::vector<RdKafka::Message*>* local_queue, bool binary_data)
{
	KafkaExport::StringValueResult Res;
	char itoa_buffer[65];
	bool headerfilter = HeaderFilter.size() > 0;
	const RdKafka::Headers* headers = nullptr;

	try {		
		std::string key_ptrn = binary_data ? "{4,\"Key\",{\"Pattern\",{\"R\"}},\"\",0}," : "{4,\"Key\",{\"Pattern\",{\"S\"}},\"\",0},";
		std::string value_ptrn = binary_data ? "{5,\"Value\",{\"Pattern\",{\"R\"}},\"\",0}," : "{5,\"Value\",{\"Pattern\",{\"S\"}},\"\",0},";
		Res.value.append("{\"#\",acf6192e-81ca-46ef-93a6-5a6968b78663,{9,{7,"
		"{0,\"Topic\",{\"Pattern\",{\"S\"}},\"\",0},"
		"{1,\"Partition\",{\"Pattern\",{\"N\"}},\"\",0},"
		"{2,\"Timestamp\",{\"Pattern\",{\"N\"}},\"\",0},"
		"{3,\"Offset\",{\"Pattern\",{\"N\"}},\"\",0},");
		Res.value.append(key_ptrn); Res.value.append(value_ptrn);
		Res.value.append("{6,\"Headers\",{\"Pattern\",{\"#\",4238019d-7e49-4fc9-91db-b6b951d5cf8e}},\"\",0}},"
		"{2,7,0,0,1,1,2,2,3,3,4,4,5,5,6,6,{1,"
		);
		sprintf(itoa_buffer, "%" PRId64, (int64_t)local_queue->size());
		Res.value.append(itoa_buffer);

		for (unsigned int i = 0; i < local_queue->size(); ++i) {
		
			sprintf(itoa_buffer, "%d", i);
			Res.value.append(",{2,");
			Res.value.append(itoa_buffer, strlen(itoa_buffer));
			Res.value.append(",7");
		
			//Topic
			internal_clmnstr(&Res.value, local_queue->at(i)->topic_name().c_str());

			//Partition
			sprintf(itoa_buffer, "%d", local_queue->at(i)->partition());
			internal_clmnnum(&Res.value, itoa_buffer);	

			//Timestamp
			sprintf(itoa_buffer, "%" PRId64, local_queue->at(i)->timestamp().timestamp);
			internal_clmnnum(&Res.value, itoa_buffer);

			//Offset
			sprintf(itoa_buffer, "%" PRId64, local_queue->at(i)->offset());
			internal_clmnnum(&Res.value, itoa_buffer);

			if (!binary_data) {
				//Key
				if (local_queue->at(i)->key())
					internal_clmnstr(&Res.value, local_queue->at(i)->key()->c_str(), local_queue->at(i)->key()->size());		
				else
					internal_clmnstr(&Res.value, "", 0);
				//Value
				internal_clmnstr(&Res.value, (const char*)local_queue->at(i)->payload(), local_queue->at(i)->len());
			}
			else {
				//Key
				if (local_queue->at(i)->key()) 
					internal_clmnbin(&Res.value, (const unsigned char*)local_queue->at(i)->key()->c_str(), (unsigned int)local_queue->at(i)->key()->size());
				else 	
					internal_clmnstr(&Res.value, "", 0);

				//Value
				internal_clmnbin(&Res.value, (const unsigned char*)local_queue->at(i)->payload(), (unsigned int)local_queue->at(i)->len());
			}

			//Headers
			Res.value.append(",{\"#\",4238019d-7e49-4fc9-91db-b6b951d5cf8e,{");
			headers = local_queue->at(i)->headers();
			if (headers) {		
					std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();	
					size_t hdrs_count = hdrs.size();	
					if (headerfilter) {
						hdrs_count = 0;
						for (size_t HeaderIndex = 0; HeaderIndex < hdrs.size(); ++HeaderIndex) {		
							if (HeaderFilter.find(hdrs[HeaderIndex].key().c_str()) != HeaderFilter.end())
								continue;
							hdrs_count++;
						}
					}
					sprintf(itoa_buffer, "%" PRId64, (int64_t)hdrs_count);
					Res.value.append(itoa_buffer);	
							
				for (size_t HeaderIndex = 0; HeaderIndex < hdrs.size(); ++HeaderIndex) {	
					if (headerfilter)
						if (HeaderFilter.find(hdrs[HeaderIndex].key().c_str()) != HeaderFilter.end())
							continue;
					//Key
					Res.value.append(",{");
					internal_clmnstr(&Res.value, hdrs[HeaderIndex].key().c_str(), hdrs[HeaderIndex].key().size(), false);
	
					//Value
					if (!binary_data) 			
						internal_clmnstr(&Res.value, hdrs[HeaderIndex].value_string(), hdrs[HeaderIndex].value_size());
					
					else 
						internal_clmnbin(&Res.value, (const unsigned char*)hdrs[HeaderIndex].value_string(), (unsigned int)hdrs[HeaderIndex].value_size());
					Res.value.append("}");
				}
			}else{Res.value.append("0");}
			Res.value.append("}},0}");
			if (RemoveMessagesFromLocalQueueOnBuild) delete local_queue->at(i);
		}
		Res.value.append("},6,");
		sprintf(itoa_buffer, "%" PRId64, (int64_t)(local_queue->size() - 1));
		Res.value.append(itoa_buffer);
		Res.value.append("},{0,0}}}");
		if (RemoveMessagesFromLocalQueueOnBuild) local_queue->clear();
	}
	catch (...) {
		Res.error = err(ERR_UNHANDLED, "onec internal string generation error");
		if (RemoveMessagesFromLocalQueueOnBuild) Res.error.fatal = true;
		return Res;
	}

	Res.succes = true;
	return Res;
}
