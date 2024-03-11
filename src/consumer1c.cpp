#include "stdafx.h"
#include "consumer1c.h"
using namespace KafkaExport::KafkaConsumer1C;
//---------------------------------------------------------------------------//
Consumer1C::Consumer1C() : ComponentBase(u"KafkaConsumer")
{
	using namespace std::placeholders;
	AddFunctionProperty(2, "Initialize", "Инициализация", std::bind(&Consumer1C::Initialize, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ClearMessagePool", "ОчиститьПулСообщений", std::bind(&Consumer1C::ClearMessagePool, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "GetMessagePoolLength", "ПолучитьКоличествоСообщенийВПуле", std::bind(&Consumer1C::GetMessagePoolLength, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "SetGlobalConf", "УстановитьНастройку", std::bind(&Consumer1C::SetGlobalConf, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "SetTopicConf", "УстановитьНастройкуТопика", std::bind(&Consumer1C::SetTopicConf, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ConfReset", "СброситьНастройки", std::bind(&Consumer1C::ConfReset, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "Subscribe", "Подписаться", std::bind(&Consumer1C::Subscribe, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "AddTopicToSubscribeList", "ДобавитьТопикВСписок", std::bind(&Consumer1C::AddTopicToSubscribeList, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ClearSubscribeList", "ОчиститьСписокТопиков", std::bind(&Consumer1C::ClearSubscribeList, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "Unsubscribe", "Отписаться", std::bind(&Consumer1C::Unsubscribe, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "Unassign", "ОтменитьНазначение", std::bind(&Consumer1C::Unassign, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "Commit", "ЗафиксироватьСмещения", std::bind(&Consumer1C::Commit, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "Assign", "Назначить", std::bind(&Consumer1C::Assign, this, _1, _2, _3), 0);
	AddFunctionProperty(3, "AddRecordToTopicPartitionList", "ДобавитьНазначение", std::bind(&Consumer1C::AddRecordToTopicPartitionList, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ClearTopicPartitionList", "ОчиститьСписокНазначений", std::bind(&Consumer1C::ClearTopicPartitionList, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "ReceiveJSONMessages", "ПолучитьСообщенияВФорматеJSON", std::bind(&Consumer1C::ReceiveJSONMessages, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "ReceiveOnecInternalMessages", "ПолучитьСообщенияВФормате1С", std::bind(&Consumer1C::ReceiveOnecInternalMessages, this, _1, _2, _3), 0);
	AddFunctionProperty(3, "QueryWatermarkOffsets", "ПолучитьСмещения", std::bind(&Consumer1C::QueryWatermarkOffsets, this, _1, _2, _3), 0);
	AddFunctionProperty(3, "CommittedOffset", "ТекущееСмещение", std::bind(&Consumer1C::CommittedOffset, this, _1, _2, _3), 0);
	AddFunctionProperty(3, "ConsumePool", "ПрочитатьСписокСообщенийВПул", std::bind(&Consumer1C::ConsumePool, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "SetLogFilePath", "УстановитьПутьКФайлуЛогов", std::bind(&Consumer1C::SetLogFilePath, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "AppendHeaderFilter", "ДобавитьФильтрЗаголовка", std::bind(&Consumer1C::AppendHeaderFilter, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ClearHeaderFilter", "ОчиститьФильтрыЗаголовков", std::bind(&Consumer1C::ClearHeaderFilter, this, _1, _2, _3), 0);

	AddProperty("RemoveMessagesFromLocalQueueOnJSONBuild", "УдалятьСообщенияИзЛокальнойОчередиПриФормировании", true, true);
	AddProperty("MaxMessagesInLocalQueue", "ДлинаЛокальнойОчереди", true, true);
	AddProperty("EscapeMessageValue", "ЭкранироватьЗначениеСообщения", true, true);
	AddProperty("EscapeMessageKey", "ЭкранироватьКлючСообщения", true, true);
	AddProperty("EscapeMessageHeaderValue", "ЭкранироватьЗначенияЗаголовковСообщения", true, true);
	AddProperty("EscapeMessageHeaderKey", "ЭкранироватьКлючиЗаголовковСообщения", true, true);

	//lasterror
	AddProperty("ErrorDescription", "ОписанниеОшибки", true, false);
	AddProperty("FatalError", "КритическаяОшибка", true, false);
}
//---------------------------------------------------------------------------//
Consumer1C::~Consumer1C()
{
}
//---------------------------------------------------------------------------//
bool Consumer1C::GetPropVal(const long lPropNum, tVariant* pvarPropVal)
{
	switch (lPropNum) {
		case 0: {
			pvarPropVal->bVal = data_builder.RemoveMessagesFromLocalQueueOnBuild;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 1: {
			pvarPropVal->dblVal = (double)kafka_consumer.max_messages_in_local_queue;
			pvarPropVal->vt = VTYPE_R8;
			break;
		}
		case 2: {
			pvarPropVal->bVal = data_builder.EscapeMessageValue;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 3: {
			pvarPropVal->bVal = data_builder.EscapeMessageKey;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 4: {
			pvarPropVal->bVal = data_builder.EscapeMessageHeaderValue;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 5: {
			pvarPropVal->bVal = data_builder.EscapeMessageHeaderKey;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 6: {
			std::string err_desc = err_to_str(LastError);
			allocString(pvarPropVal, err_desc.c_str(), err_desc.size());
			break;
		}
		case 7: {
			pvarPropVal->bVal = LastError.fatal;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		default: {
			return false;
		}
	}
	return true;
}
//---------------------------------------------------------------------------//
bool Consumer1C::SetPropVal(const long lPropNum, tVariant* varPropVal)
{
	switch (lPropNum) {
		case 0: {
			if (varPropVal->vt != VTYPE_BOOL) return false;
			data_builder.RemoveMessagesFromLocalQueueOnBuild = varPropVal->bVal;
			break;
		}
		case 1: {
			if ((varPropVal->intVal > MAX_MESSAGES_IN_LOCAL_QUEUE) || (varPropVal->intVal <= 0)) return false;
			kafka_consumer.max_messages_in_local_queue = varPropVal->intVal;
			break;
		}
		case 2: {
			if (varPropVal->vt != VTYPE_BOOL) return false;
			data_builder.EscapeMessageValue = varPropVal->bVal;
			break;
		}
		case 3: {
			if (varPropVal->vt != VTYPE_BOOL) return false;
			data_builder.EscapeMessageKey = varPropVal->bVal;
			break;
		}
		case 4: {
			if (varPropVal->vt != VTYPE_BOOL) return false;
			data_builder.EscapeMessageHeaderValue = varPropVal->bVal;
			break;
		}
		case 5: {
			if (varPropVal->vt != VTYPE_BOOL) return false;
			data_builder.EscapeMessageHeaderKey = varPropVal->bVal;
			break;
		}
		default: {
			return false;
		}
	}
	return true;
}

//---------------------------------------------------------------------------//
bool Consumer1C::Initialize(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
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
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	std::string brokers = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	std::string groupId = toUTF8String((paParams + 1)->pwstrVal, (paParams + 1)->wstrLen);

	RetValue InitRes = kafka_consumer.Initialize(brokers, groupId);
	if (!InitRes.succes) {
		SetError(InitRes.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::SetGlobalConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
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
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	std::string key = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	std::string value = toUTF8String((paParams + 1)->pwstrVal, (paParams + 1)->wstrLen);

	RetValue Res = kafka_consumer.SetGlobalConf(key, value);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::SetTopicConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
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
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	std::string key = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	std::string value = toUTF8String((paParams + 1)->pwstrVal, (paParams + 1)->wstrLen);

	RetValue res = kafka_consumer.SetTopicConf(key, value);;
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::ConfReset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue res;
	res = kafka_consumer.ConfReset();
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}

//---------------------------------------------------------------------------//
bool Consumer1C::ClearMessagePool(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	bool ret = true;

	kafka_consumer.ClearLocalQueue();
	pvarRetValue->bVal = true;

	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::AddTopicToSubscribeList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;
	std::string topic;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}
	
	if (TopicList.size() >= VECTOR_SIZELIMIT) {
		SetError(err(ERR_SIZELIMIT, "topic list overflow"));
		return ret;
	}

	topic = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	bool Add = true;
	for (unsigned int i = 0; i < TopicList.size(); ++i) {
		if (TopicList[i] == topic) {
			Add = false;
			break;
		}
	}

	if (Add) {
		try {
			TopicList.push_back(topic);
		}
		catch (...) {
			SetError(err(ERR_BADALLOC, "topic list adding error"));
			return ret;
		}
	}
	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::Subscribe(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue Res = kafka_consumer.Subscribe(&TopicList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = Res.succes;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::ClearSubscribeList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	TopicList.clear();
	TopicList.shrink_to_fit();

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::Unsubscribe(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue Res;
	Res = kafka_consumer.Unsubscribe();
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::AddRecordToTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;
	bool ValIsEmpty = false;
	int32_t partition = -1;
	int64_t offset = -1;
	std::string topic;

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
	
	if (TopicPartitionList.size() >= VECTOR_SIZELIMIT) {
		SetError(err(ERR_SIZELIMIT, "topic-partition list overflow"));
		return ret;
	}

	topic = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
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
			SetError(err(ERR_BADALLOC, "TopicPartitionList.push_back error"));
			return ret;
		}
	}
	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::Assign(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue Res = kafka_consumer.Assign(&TopicPartitionList);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::Unassign(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue res;
	res = kafka_consumer.Unassign();
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::ClearTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
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
bool Consumer1C::ConsumePool(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_I4;
	pvarRetValue->bVal = false;
	bool ret = true;

	unsigned int timeout;
	int32_t errors_count_to_interrupt = 0;
	int32_t count = 0;
	try {
		if (!tVariantIsNumber(paParams)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 2)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	timeout = (paParams)->uintVal;
	count = (paParams + 1)->lVal;
	errors_count_to_interrupt = (paParams + 2)->lVal;

	RetValue res = kafka_consumer.ConsumePool(count, errors_count_to_interrupt, timeout);
	if (res.error.fatal) {
		SetError(res.error);
		pvarRetValue->intVal = CONSUMER_FATAL_ERROR;
		return ret;
	}
	if (!res.succes) {
		pvarRetValue->intVal = CONSUMER_ERROR;
		SetError(res.error);
		return ret;
	}

	pvarRetValue->intVal = CONSUMER_NO_ERROR;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::GetMessagePoolLength(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_I4;
	pvarRetValue->lVal = -1;
	bool ret = true;

	int32_t res = (int32_t)kafka_consumer.local_queue.size();
	pvarRetValue->lVal = res;

	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::Commit(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue res;
	res = kafka_consumer.Commit();
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::ReceiveJSONMessages(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;
	bool base64 = false;

	try {
		if ((paParams)->vt != VTYPE_BOOL) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	base64 = (paParams)->bVal;
	StringValueResult Res = data_builder.JSON_KafkaMessagePool(&kafka_consumer.local_queue, base64);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::ReceiveOnecInternalMessages(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;
	bool base64 = false;

	try {
		if ((paParams)->vt != VTYPE_BOOL) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	base64 = (paParams)->bVal;
	StringValueResult Res = data_builder.OnecInternal_KafkaMessagePool(&kafka_consumer.local_queue, base64);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	allocString(pvarRetValue, Res.value.c_str(), Res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::AppendHeaderFilter(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;
	std::string header;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}
	
	header = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	RetValue Res = data_builder.AppendHeaderFilter(header);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::ClearHeaderFilter(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue Res = data_builder.ClearHeaderFilter();
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::QueryWatermarkOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 2)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	std::string topic = toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	RetOffsets offsets = kafka_consumer.QueryWatermarkOffsets(topic, (paParams + 1)->lVal, (paParams + 2)->lVal);
	if (!offsets.succes) {
		SetError(offsets.error);
		return ret;
	}

	KafkaExport::StringValueResult res = data_builder.JSON_GetJSONWatermarkOffsets(offsets);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	allocString(pvarRetValue, res.value.c_str(), res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::CommittedOffset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 1)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 2)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	std::string topic= toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen);
	int64ValueResult committed = kafka_consumer.Committed(topic, (paParams + 1)->lVal, (paParams + 2)->lVal);
	if (!committed.succes) {
		SetError(err(ERR_UNHANDLED, committed.error.description, committed.error.fatal));
		return ret;
	}
	std::string t = std::to_string(committed.value);
	allocString(pvarRetValue, t.c_str(), t.size());

	return ret;
}
//---------------------------------------------------------------------------//
bool Consumer1C::SetLogFilePath(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) {
			throw std::invalid_argument(0);
		}
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}
	
	RetValue Res = kafka_consumer.SetLogFilePath(toUTF8String((paParams)->pwstrVal, (paParams)->wstrLen));
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
void Consumer1C::SetError(KafkaExport::ErrorDescription error)
{
	LastError = error;
}
//---------------------------------------------------------------------------//
void Consumer1C::ClearError()
{
	LastError.type = ERR_SUCCESS;
	LastError.fatal = false;
	LastError.description.clear();
}
//---------------------------------------------------------------------------//
