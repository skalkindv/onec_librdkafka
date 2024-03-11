#include "stdafx.h"
#include "data_conversion.h"
using namespace KafkaExport::DataConversion;

//---------------------------------------------------------------------------//
DataConteiner::DataConteiner()
{
	json = nlohmann::json();
}
//---------------------------------------------------------------------------//
DataConteiner::~DataConteiner()
{
	json.clear();
}
//---------------------------------------------------------------------------//
KafkaExport::RetValue DataConteiner::Load(std::string JSON)
{
	RetValue res;
	checked = false;
	if (!nlohmann::json::accept(JSON)) {
		res.error = err(ERR_JSONPARSING, "invalid json");
		return res;
	}
	try {
		json = nlohmann::json::parse(JSON);
	}
	catch (...) {
		res.error = err(ERR_JSONPARSING, "json parsing error");
		return res;
	}

	if (!JSONStructureCheck()) {
		res.error = err(ERR_JSONPARSING, "invalid json structure");
		return res;
	}
	checked = true;
	res.succes = true;
	return res;
}
//---------------------------------------------------------------------------//
void DataConteiner::Clear()
{
	checked = false;
	json.clear();
}
//---------------------------------------------------------------------------//
size_t DataConteiner::ElementsCount()
{
	return json.size();
}
//---------------------------------------------------------------------------//
bool DataConteiner::IsValid()
{
	return checked;
}
//---------------------------------------------------------------------------//
bool DataConteiner::JSONStructureCheck()
{
	if (!json.is_array()) {
		return false;
	}
	for (unsigned int i = 0; i < json.size(); ++i) {

		if (!json[i].contains("Key") || (!json[i].contains("Value"))) {
			return false;
		}
		if (CheckJSONFieldsForStringType) {
			if ((!json[i]["Key"].is_string()) || (!json[i]["Value"].is_string())) {
				return false;
			}
		}
		if (json[i].contains("Headers")) {
			if (!json[i]["Headers"].is_array()) {
				return false;
			}
			for (unsigned int headers_count = 0; headers_count < json[i]["Headers"].size(); ++headers_count) {
				if (!json[i]["Headers"][headers_count].is_object()) {
					return false;
				}
				if (CheckJSONFieldsForStringType) {
					for (auto it = json[i]["Headers"][headers_count].begin(); it != json[i]["Headers"][headers_count].end(); ++it) {
						if (!it.value().is_string()) {
							return false;
						}
					}
				}
			}
		}
	}
	return true;
}
//---------------------------------------------------------------------------//
size_t DataConteiner::GetHeadersCountByIndex(uint32_t index)
{
	if (!HeadersContains(index)) {
		return -1;
	}
	return json[index]["Headers"].size();
}
//---------------------------------------------------------------------------//
bool DataConteiner::HeadersContains(uint32_t index)
{
	if (!checked) {
		return false;
	}
	if (json.size() <= index) {
		return false;
	}

	return json[index].contains("Headers");
}
//---------------------------------------------------------------------------//
bool DataConteiner::GetKeyByIndex(uint32_t index, std::string *dest)
{
	if (!checked) {
		return false;
	}
	if (json.size() <= index) {
		return false;
	}

	if (!DecodeBase64Key) {
		if (json[index]["Key"].is_string())
			dest->append(json[index]["Key"].get<std::string>().c_str());
		else
			dest->append(json[index]["Key"].dump().c_str());
	}
	else {
		if (json[index]["Key"].is_string())
			base64_decode_(json[index]["Key"].get<std::string>(), dest);
		else
			base64_decode_(json[index]["Key"].dump().c_str(), dest);
	}

	return true;
}
//---------------------------------------------------------------------------//
bool DataConteiner::GetValueByIndex(uint32_t index, std::string* dest)
{
	if (!checked) {
		return false;
	}
	if (json.size() <= index) {
		return false;
	}

	if (!DecodeBase64Value) {
		if (json[index]["Value"].is_string())
			dest->append(json[index]["Value"].get<std::string>().c_str());
		else
			dest->append(json[index]["Value"].dump().c_str());
	}
	else {
		if (json[index]["Value"].is_string())
			base64_decode_(json[index]["Value"].get<std::string>().c_str(), dest);
		else
			base64_decode_(json[index]["Value"].dump().c_str(), dest);
	}
	return true;
}
//---------------------------------------------------------------------------//
bool DataConteiner::GetHeadersByIndex(uint32_t index, RdKafka::Headers* dest)
{
	if (!checked) {
		return false;
	}
	if (json.size() <= index) {
		return false;
	}
	if (!json[index].contains("Headers")) {
		return false;
	}

	for (unsigned int headers_count = 0; headers_count < json[index]["Headers"].size(); ++headers_count) {
		for (auto it = json[index]["Headers"][headers_count].begin(); it != json[index]["Headers"][headers_count].end(); ++it) {
			if (!DecodeBase64HeadersValue) {
				if (it.value().is_string())
					dest->add(it.key(), it.value().get<std::string>());
				else
					dest->add(it.key(), it.value().dump());
			}
			else {
				std::string header_value;
				if (it.value().is_string())
					base64_decode_(it.value().get<std::string>(), &header_value);
				else
					base64_decode_(it.value().dump(), &header_value);
				dest->add(it.key(), header_value.c_str(), header_value.size());
			}
		}
	}
	return true;
}
//---------------------------------------------------------------------------//
