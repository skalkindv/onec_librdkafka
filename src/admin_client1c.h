#ifndef __ADMINCLIENT1C__
#define __ADMINCLIENT1C__

#include "ComponentBaseImp.h"
#include "component_types.h"
#include "data_conversion.h"

///////////////////////////////////////////////////////////////////////////////
namespace KafkaExport
{
	namespace KafkaAdminClient1C {

		class KafkaAdminClientCore
		{

		private:	
			std::string brokers;
			rd_kafka_t *rk = nullptr;
			rd_kafka_conf_t *conf = nullptr;	
			rd_kafka_conf_t *rk_conf = nullptr;	

			std::string errstr;
			char errbuf[512];
			bool Init = false;

		public:
			KafkaAdminClientCore();
			virtual ~KafkaAdminClientCore();

			bool IsInit();
			RetValue Initialize(std::string brokers_);
			RetValue ConfReset();
			RetValue GlobalConfDefaultInit(std::string _brokers);
			RetValue SetGlobalConf(std::string key, std::string value);
			TopicPartitionListResult DeleteRecordsBefore(std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, int32_t timeout_ms);
			TopicPartitionListResult GetGroupOffsets(std::string group_id, int32_t timeout_ms);
			GroupListResult GetGroupList(int32_t timeout_ms);
			MetadataResult GetMetadata(int32_t timeout_ms, std::string topic);
			TopicPartitionListResult AlterGroupOffsets(std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, std::string group_id, int32_t timeout_ms);
			TopicPartitionListResult DeleteGroupOffsets(std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, std::string group_id, int32_t timeout_ms);
			WatermarkOffsetsListResult QueryWatermarkOffsets(std::vector<KafkaExport::TopicPartitionDescription>* TopicPartitionList, int32_t timeout_ms);
		};

		class AdminClient1C : public ComponentBase
		{
		public:
			AdminClient1C();
			virtual ~AdminClient1C();
			virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal);
			virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* pvarPropVal);

		private:
			KafkaExport::ErrorDescription LastError;
			
			//params
			std::vector<TopicPartitionDescription> TopicPartitionList;

			//kafka class
			KafkaAdminClientCore kafka_admin_client;
			DataConversion::DataBuilder data_builder;

			// 1c Interface
			bool Initialize(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetGlobalConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ConfReset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool AddRecordToTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ClearTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool DeleteRecordsBefore(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool GetGroupOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool GetGroupList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool GetMetadata(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool AlterGroupOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool DeleteGroupOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool QueryWatermarkOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			void SetError(KafkaExport::ErrorDescription error);
			void ClearError();
		};
	}
}

#endif
