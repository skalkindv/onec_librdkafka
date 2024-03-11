#ifndef __PRODUCER1C_H__
#define __PRODUCER1C_H__

#include "ComponentBaseImp.h"
#include "component_types.h"
#include "data_conversion.h"

///////////////////////////////////////////////////////////////////////////////
namespace KafkaExport
{
	namespace KafkaProducerc1C
	{
		class KafkaProducerCore
		{

		private:
			class MyDeliveryReportCb : public RdKafka::DeliveryReportCb
			{
			public:
				std::vector<delivery_record> records;

				MyDeliveryReportCb();
				virtual ~MyDeliveryReportCb();
				void ClearRecords();
				RetValue AddRecord(std::string key, std::string status, std::string topic, int32_t partition, int64_t timestamp, int64_t offset, std::string error);
				void dr_cb(RdKafka::Message& message);
			};

			RdKafka::Conf* conf = nullptr;
			RdKafka::Conf* tconf = nullptr;

			std::string brokers;
			std::string topic;
			int32_t partition = -1;

			RdKafka::Producer* producer = nullptr;

			std::string errstr;
			bool Init = false;

		public:
			KafkaProducerCore();
			virtual ~KafkaProducerCore();

			MyDeliveryReportCb dr_cb;

			RetValue Initialize(std::string _brokers, std::string _topic, int32_t partition = RdKafka::Topic::PARTITION_UA);
			RetValue GlobalConfDefaultInit(std::string _brokers = "");
			RetValue ConfReset();
			RetValue SetGlobalConf(std::string key, std::string value);
			RetValue SetTopicConf(std::string key, std::string value);
			RetValue Produce(KafkaExport::DataConversion::DataConteiner *json_conteiner);
			BoolValueResult IsDelivered();
			bool IsInit();
		};

		class Producer1C : public ComponentBase
		{
		public:

			Producer1C();
			virtual ~Producer1C();
			virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal);
			virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* pvarPropVal);

		private:
			KafkaExport::ErrorDescription LastError;

			KafkaProducerCore kafka_producer;
			DataConversion::DataConteiner json_conteiner;
			DataConversion::DataBuilder data_builder;

			// 1c Interface
			bool Initialize(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetGlobalConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetTopicConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ConfReset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetJSONMessageList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ClearMessagePool(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool GetMessagePoolLength(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool Produce(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool GetJSONDeliveryReport(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool IsDelivered(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			//error handling
			void SetError(KafkaExport::ErrorDescription error);
			void ClearError();
		};
	}
}

#endif
