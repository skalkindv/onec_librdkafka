#include "stdafx.h"
#include "producer1c.h"

//---------------------------------------------------------------------------//
using namespace KafkaExport::KafkaProducerc1C;
Producer1C::Producer1C() : ComponentBase(u"KafkaProducer")
{
	using namespace std::placeholders;
	AddFunctionProperty(3, "Initialize", "Инициализация", std::bind(&Producer1C::Initialize, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "Produce", "Отправить", std::bind(&Producer1C::Produce, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ClearMessagePool", "ОчиститьПулСообщений", std::bind(&Producer1C::ClearMessagePool, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "GetMessagePoolLength", "ПолучитьКоличествоСообщенийВПуле", std::bind(&Producer1C::GetMessagePoolLength, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "SetGlobalConf", "УстановитьНастройку", std::bind(&Producer1C::SetGlobalConf, this, _1, _2, _3), 0);
	AddFunctionProperty(2, "SetTopicConf", "УстановитьНастройкуТопика", std::bind(&Producer1C::SetTopicConf, this, _1, _2, _3), 0);
	AddFunctionProperty(1, "SetJSONMessageList", "ЗагрузитьСписокСообщенийИзJSON", std::bind(&Producer1C::SetJSONMessageList, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "ConfReset", "СброситьНастройки", std::bind(&Producer1C::ConfReset, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "GetJSONDeliveryReport", "ПолучитьОтчетОДоставкеJSON", std::bind(&Producer1C::GetJSONDeliveryReport, this, _1, _2, _3), 0);
	AddFunctionProperty(0, "IsDelivered", "УспешноДоставлено", std::bind(&Producer1C::IsDelivered, this, _1, _2, _3), 0);

	AddProperty("DecodeBase64Key", "ДекодироватьКлючиСообщенийИзBase64", true, true);
	AddProperty("DecodeBase64Value", "ДекодироватьСообщенияИзBase64", true, true);
	AddProperty("DecodeBase64HeadersValue", "ДекодироватьЗаголовкиСообщенийИзBase64", true, true);
	AddProperty("CheckJSONFieldsForStringType", "ПроверятьЯвляеютсяЛиПоляJSONСтроками", true, true);
	AddProperty("ErrorDescription", "ОписаниеОшибки", true, false);
}
//---------------------------------------------------------------------------//
Producer1C::~Producer1C()
{
}
//---------------------------------------------------------------------------//
bool Producer1C::GetPropVal(const long lPropNum, tVariant* pvarPropVal)
{
	switch (lPropNum) {
		case 0: {
			pvarPropVal->bVal = json_conteiner.DecodeBase64Key;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 1: {
			pvarPropVal->bVal = json_conteiner.DecodeBase64Value;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 2: {
			pvarPropVal->bVal = json_conteiner.DecodeBase64HeadersValue;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 3: {
			pvarPropVal->bVal = json_conteiner.CheckJSONFieldsForStringType;
			pvarPropVal->vt = VTYPE_BOOL;
			break;
		}
		case 4: {
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
bool Producer1C::SetPropVal(const long lPropNum, tVariant* varPropVal)
{
	switch (lPropNum) {
		case 0: {
			if (varPropVal->vt != VTYPE_BOOL) return false;

			json_conteiner.DecodeBase64Key = varPropVal->bVal;
			break;
		}
		case 1: {
			if (varPropVal->vt != VTYPE_BOOL)  return false;

			json_conteiner.DecodeBase64Value = varPropVal->bVal;
			break;
		}
		case 2: {
			if (varPropVal->vt != VTYPE_BOOL)  return false;

			json_conteiner.DecodeBase64HeadersValue = varPropVal->bVal;
			break;
		}
		case 3: {
			if (varPropVal->vt != VTYPE_BOOL)  return false;

			json_conteiner.CheckJSONFieldsForStringType = varPropVal->bVal;
			break;
		}
		default: {
			return false;
		}
	}
	return true;
}
//---------------------------------------------------------------------------//
bool Producer1C::Initialize(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (((paParams + 1)->pwstrVal == nullptr) || ((paParams + 1)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
		if (!tVariantIsNumber(paParams + 2)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	std::string brokers = toUTF8String(TV_WSTR(paParams), (paParams)->wstrLen);
	std::string topic_str = toUTF8String(TV_WSTR((paParams + 1)), (paParams + 1)->wstrLen);
	int32_t partition = (paParams + 2)->lVal;

	RetValue Res = kafka_producer.Initialize(brokers, topic_str, partition);
	if (!Res.succes) {
		SetError(Res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::Produce(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue res = kafka_producer.Produce(&json_conteiner);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}
	json_conteiner.Clear();
	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::SetJSONMessageList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	try {
		if (((paParams)->pwstrVal == nullptr) || ((paParams)->vt != VTYPE_PWSTR)) throw std::invalid_argument(0);
	}
	catch (...) {
		SetError(err(ERR_BADPARAMETR, "invalid types"));
		return ret;
	}

	RetValue res = json_conteiner.Load(toUTF8String(TV_WSTR(paParams), (paParams)->wstrLen));
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::ClearMessagePool(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;
	json_conteiner.Clear();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = true;

	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::GetMessagePoolLength(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();

	TV_VT(pvarRetValue) = VTYPE_I4;
	pvarRetValue->lVal = (int32_t)json_conteiner.ElementsCount();

	return true;
}

//---------------------------------------------------------------------------//
bool Producer1C::SetGlobalConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
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

	std::string key = toUTF8String(TV_WSTR(paParams), (paParams)->wstrLen);
	std::string value = toUTF8String(TV_WSTR((paParams + 1)), (paParams + 1)->wstrLen);
	
	RetValue res = kafka_producer.SetGlobalConf(key, value);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::SetTopicConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
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

	std::string key = toUTF8String(TV_WSTR(paParams), (paParams)->wstrLen);
	std::string value = toUTF8String(TV_WSTR((paParams + 1)), (paParams + 1)->wstrLen);

	RetValue res = kafka_producer.SetTopicConf(key, value);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}

//---------------------------------------------------------------------------//
bool Producer1C::ConfReset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	pvarRetValue->bVal = false;
	bool ret = true;

	RetValue res;
	res = kafka_producer.ConfReset();
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}

	pvarRetValue->bVal = true;
	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::GetJSONDeliveryReport(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;

	TV_VT(pvarRetValue) = VTYPE_PWSTR;
	KafkaExport::StringValueResult res = data_builder.JSON_DeliveryReport(kafka_producer.dr_cb.records, json_conteiner.DecodeBase64Key);
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}
	allocString(pvarRetValue, res.value.c_str(), res.value.size());
	return ret;
}
//---------------------------------------------------------------------------//
bool Producer1C::IsDelivered(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	ClearError();
	bool ret = true;
	TV_VT(pvarRetValue) = VTYPE_BOOL;
	
	BoolValueResult res = kafka_producer.IsDelivered();
	if (!res.succes) {
		SetError(res.error);
		return ret;
	}
	pvarRetValue->bVal = res.value;
	return ret;
}
//---------------------------------------------------------------------------//
void Producer1C::SetError(KafkaExport::ErrorDescription error)
{
	LastError = error;
}
//---------------------------------------------------------------------------//
void Producer1C::ClearError()
{
	LastError.type = ERR_SUCCESS;
	LastError.fatal = false;
	LastError.description.clear();
}
//---------------------------------------------------------------------------//
