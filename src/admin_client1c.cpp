#include "stdafx.h"
#include "admin_client1c.h"
using namespace KafkaExport::KafkaAdminClient1C;
//---------------------------------------------------------------------------//
AdminClient1C::AdminClient1C() : ComponentBase(u"KafkaAdminClient")
{
	using namespace std::placeholders;
	AddFunctionProperty(1, "Initialize", "Инициализация", std::bind(&AdminClient1C::Initialize, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "SetGlobalConf", "УстановитьНастройку", std::bind(&AdminClient1C::SetGlobalConf, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ConfReset", "СброситьНастройки", std::bind(&AdminClient1C::ConfReset, this, _1, _2, _3), 0);
	AddFunctionProperty(3, "AddRecordToTopicPartitionList", "ДобавитьНазначение", std::bind(&AdminClient1C::AddRecordToTopicPartitionList, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ClearTopicPartitionList", "ОчиститьСписокНазначений", std::bind(&AdminClient1C::ClearTopicPartitionList, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "DeleteRecordsBefore", "УдалитьСообщенияДоСмещения", std::bind(&AdminClient1C::DeleteRecordsBefore, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "GetGroupOffsets", "ПолучитьСмещенияГруппы", std::bind(&AdminClient1C::GetGroupOffsets, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "GetGroupList", "ПолучитьСписокГрупп", std::bind(&AdminClient1C::GetGroupList, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "GetMetadata", "ПолучитьМетаданные", std::bind(&AdminClient1C::GetMetadata, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "AlterGroupOffsets", "ИзменитьСмещенияГруппы", std::bind(&AdminClient1C::AlterGroupOffsets, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "DeleteGroupOffsets", "УдалитьСмещенияГруппы", std::bind(&AdminClient1C::DeleteGroupOffsets, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "QueryWatermarkOffsets", "ПолучитьСмещенияРазделов", std::bind(&AdminClient1C::QueryWatermarkOffsets, this, _1, _2, _3), 0);
	
	//lasterror
	AddProperty("ErrorDescription", "ErrorDescription", true, false);
}
//---------------------------------------------------------------------------//
AdminClient1C::~AdminClient1C()
{
}
//---------------------------------------------------------------------------//
bool AdminClient1C::GetPropVal(const long lPropNum, tVariant* pvarPropVal)
{
	switch (lPropNum) {
		case 0: {
			std::string err_desc = err_to_str(LastError);
			allocString(pvarPropVal, err_desc.c_str(), err_desc.size());
			break;
		}
		default: {
			return false;
		}
	}
	return true;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::SetPropVal(const long lPropNum, tVariant* varPropVal)
{
	return false;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::Initialize(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{

	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid broker address"));
		return ret;
	}

	std::string brokers = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	RetValue InitRes = kafka_admin_client.Initialize(brokers);
	if (!InitRes.succes) {
		SetError(InitRes.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::SetGlobalConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (((paParams + 1)->pwstrVal == nullptr) || ((paParams + 1)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid configuration"));
		return ret;
	}

	std::string key = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	std::string value = toUTF8String((paParams + 1)->pwstrVal, (paParams + 1)->wstrLen);

	RetValue Res = kafka_admin_client.SetGlobalConf(key, value);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::ConfReset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue res = kafka_admin_client.ConfReset();
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::AddRecordToTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	bool ValIsEmpty = false;
	int32_t partition = -1;
	int64_t offset = -1;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);

		ValIsEmpty = (paParams + 2)->vt == VTYPE_EMPTY;
		if ((!tVariantIsNumber(paParams + 2)) & !ValIsEmpty) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	partition = (paParams + 1)->lVal;
	if (!ValIsEmpty) {
		if (((paParams + 2)->vt == VTYPE_R4) || ((paParams + 2)->vt == VTYPE_R8)) {
			offset = (int64_t)(paParams + 2)->dblVal;
		}
		else {
			offset = (paParams + 2)->llVal;
		}
	}

	std::string topic = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	if (topic.empty()) {
		SetError(err(ERR_BADPARAMETR, "empty topic name"));
		return ret;
	}

	if (TopicPartitionList.size() >= VECTOR_SIZELIMIT) {
		SetError(err(ERR_SIZELIMIT, "topic-partition list overflow"));
		return ret;
	}

	bool Add = true;
	for (unsigned int i = 0; i < TopicPartitionList.size(); ++i) {
		if ((TopicPartitionList[i].topic == topic) & (TopicPartitionList[i].partition == partition)) {
			TopicPartitionList[i].offset = offset;
			Add = false;
			break;
		}
	}
	if (Add) {
		TopicPartitionDescription TopicPartition;

		TopicPartition.topic = topic;
		TopicPartition.partition = partition;
		TopicPartition.offset = offset;

		try {
			TopicPartitionList.push_back(TopicPartition);
		}
		catch (...) {
			SetError(err(ERR_BADALLOC, "topic-partition list adding error"));
			return ret;
		}
	}
	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::ClearTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	TopicPartitionList.clear();

	pvarRetValue->bVal = true;
	return ret;
}

//---------------------------------------------------------------------------//
bool AdminClient1C::DeleteRecordsBefore(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;
	unsigned int timeout_ms = -1;

	try {
		if (!tVariantIsNumber(paParams)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	timeout_ms = (paParams)->lVal;
	TopicPartitionListResult res = kafka_admin_client.DeleteRecordsBefore(&TopicPartitionList, timeout_ms);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	StringValueResult Res = data_builder.JSON_TopicPartitionList(&res.TopicPartitionList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::GetGroupOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;
	unsigned int timeout_ms = -1;
	std::string group_id;
	
	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	timeout_ms = (paParams + 1)->lVal;
	group_id = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	
	TopicPartitionListResult res = kafka_admin_client.GetGroupOffsets(group_id, timeout_ms);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	StringValueResult Res = data_builder.JSON_TopicPartitionList(&res.TopicPartitionList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::GetGroupList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;	
	unsigned int timeout_ms = -1;
	
	try {
		if (!tVariantIsNumber(paParams)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}
	
	timeout_ms = (paParams)->lVal;
	GroupListResult res = kafka_admin_client.GetGroupList(timeout_ms);
	SetError(res.error); //always return error description
	if (!res.succes) {
		return ret;
	}

	StringValueResult Res = data_builder.JSON_GroupList(&res.GroupList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::GetMetadata(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;
	bool ValIsEmpty = true;
	unsigned int timeout_ms = -1;
	std::string topic;	
	try {	
		if (!tVariantIsNumber(paParams)) throw std::invalid_argument(0);
		
		ValIsEmpty = (paParams + 1)->vt == VTYPE_EMPTY;
		if ((((paParams + 1)->pwstrVal == nullptr) || ((paParams + 1)->vt != VTYPE_PWSTR)) 
			& (!ValIsEmpty)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}
	
	timeout_ms = (paParams)->lVal;
	if (!ValIsEmpty){
		topic = toUTF8String((paParams + 1)->pwstrVal, (paParams + 1)->wstrLen);
	}
			
	MetadataResult res = kafka_admin_client.GetMetadata(timeout_ms, topic);	
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	StringValueResult Res = data_builder.JSON_Metadata(&res.Metadata);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::AlterGroupOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;
	std::string group_id;
	unsigned int timeout_ms = -1;
	
	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	timeout_ms = (paParams + 1)->lVal;
	group_id = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	
	TopicPartitionListResult res = kafka_admin_client.AlterGroupOffsets(&TopicPartitionList, group_id, timeout_ms);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	StringValueResult Res = data_builder.JSON_TopicPartitionList(&res.TopicPartitionList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::DeleteGroupOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;
	std::string group_id;
	unsigned int timeout_ms = -1;
	
	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	timeout_ms = (paParams + 1)->lVal;
	group_id = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	
	TopicPartitionListResult res = kafka_admin_client.DeleteGroupOffsets(&TopicPartitionList, group_id, timeout_ms);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	StringValueResult Res = data_builder.JSON_TopicPartitionList(&res.TopicPartitionList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool AdminClient1C::QueryWatermarkOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	pvarRetValue->bVal = false;
	bool ret = true;
	unsigned int timeout_ms = -1;
	
	try {
		if (!tVariantIsNumber(paParams)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	timeout_ms = (paParams)->lVal;
	WatermarkOffsetsListResult res = kafka_admin_client.QueryWatermarkOffsets(&TopicPartitionList, timeout_ms);	
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}
	
	StringValueResult Res = data_builder.JSON_WatermarkOffsets(&res.WatermarkOffsetsList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}
	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
void AdminClient1C::SetError(KafkaExport::ErrorDescription error)
{
	LastError = error;
}
//---------------------------------------------------------------------------//
void AdminClient1C::ClearError()
{
	LastError.type = ERR_SUCCESS;
	LastError.fatal = false;
	LastError.description.clear();
}
//---------------------------------------------------------------------------//
