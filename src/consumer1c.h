#ifndef __CONSUMER1C__
#define __CONSUMER1C__

#include "ComponentBaseImp.h"
#include "component_types.h"
#include "data_conversion.h"
#include <fstream>

///////////////////////////////////////////////////////////////////////////////
namespace KafkaExport
{
	namespace KafkaConsumer1C {

		class KafkaConsumerCore1C
		{

		private:
			class MyEventCb : public RdKafka::EventCb {
			private:
				std::string log_path;
				bool enabled = false;

			public:
				MyEventCb();
				virtual ~MyEventCb();

				void event_cb(RdKafka::Event& event);
				bool enable(std::string);
				void disable();
			};

			class MyRebalanceCb : public RdKafka::RebalanceCb {
			public:

				MyRebalanceCb();
				virtual ~MyRebalanceCb();
				void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*>& partitions);
			};

			RdKafka::Conf* conf = nullptr;
			RdKafka::Conf* tconf = nullptr;

			std::string brokers;
			std::string groupId;
			RdKafka::KafkaConsumer* consumer = nullptr;
			std::vector<RdKafka::TopicPartition*> TopicPatrtitionCurrentAssignments;

			std::string errstr;
			bool Init = false;

			void ClearCurrentAssignmentsList();

		public:
			KafkaConsumerCore1C();
			virtual ~KafkaConsumerCore1C();

			MyRebalanceCb rb_cb;
			MyEventCb event_cb;

			std::vector<RdKafka::Message*> local_queue;
			size_t max_messages_in_local_queue = MAX_MESSAGES_IN_LOCAL_QUEUE;
			std::string log_path;

			bool IsInit();
			RetValue Initialize(std::string brokers_, std::string groupId_);
			RetValue Subscribe(std::vector<std::string> *Topics);
			RetValue Unsubscribe();
			RetValue Assign(std::vector<TopicPartitionDescription> *TopicPartitionList);
			RetValue Unassign();
			RetValue Commit();
			RetValue Consume(unsigned int timeout);
			bool AddMessage(RdKafka::Message* message);
			RetValue GlobalConfDefaultInit(std::string _brokers, std::string groupId);
			RetValue ConfReset();
			RetValue SetGlobalConf(std::string key, std::string value);
			RetValue SetTopicConf(std::string key, std::string value);
			RetOffsets QueryWatermarkOffsets(std::string topic, int32_t partition, int32_t timeoutms);
			int64ValueResult Committed(std::string topic, int32_t partition, int32_t timeoutms);
			RetValue ConsumePool(int32_t count, int32_t errors_count_to_interrupt, unsigned int timeout);
			void ClearLocalQueue();
			RetValue SetLogFilePath(std::string fileName);
		};

		class Consumer1C : public ComponentBase
		{
		public:

			Consumer1C();
			virtual ~Consumer1C();
			virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal);
			virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* pvarPropVal);

		private:

			ErrorDescription LastError;

			//messages
			std::vector<std::string> TopicList;
			std::vector<TopicPartitionDescription> TopicPartitionList;

			//kafka class
			KafkaConsumerCore1C kafka_consumer;
			DataConversion::DataBuilder data_builder;

			// 1c Interface
			bool Initialize(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetGlobalConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetTopicConf(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ConfReset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool Subscribe(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool AddTopicToSubscribeList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ConsumePool(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ClearMessagePool(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ClearSubscribeList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool GetMessagePoolLength(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool Unsubscribe(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool AddRecordToTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool Assign(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool Unassign(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ClearTopicPartitionList(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool Commit(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ReceiveJSONMessages(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ReceiveOnecInternalMessages(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool QueryWatermarkOffsets(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool CommittedOffset(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool SetLogFilePath(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool AppendHeaderFilter(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			bool ClearHeaderFilter(tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
			void SetError(KafkaExport::ErrorDescription error);
			void ClearError();
		};
	}
}

#endif
