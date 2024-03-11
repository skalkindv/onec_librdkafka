// kafka_export.cpp : Defines the exported functions for the DLL application.
//

#include "stdafx.h"
#include "export.h"

//***************************1c_Exports**************************************//
//---------------------------------------------------------------------------//
long GetClassObject(const WCHAR_T* wsName, IComponentBase** pInterface)
{
	if (!*pInterface) {
	auto cls_name = std::u16string(reinterpret_cast<const char16_t *>(wsName));			
		if (cls_name == u"KafkaProducer") {
			*pInterface = new KafkaExport::KafkaProducerc1C::Producer1C();
		}
		else if (cls_name == u"KafkaConsumer"){
			*pInterface = new KafkaExport::KafkaConsumer1C::Consumer1C();
		}	
		else if (cls_name == u"KafkaAdminClient"){
			*pInterface = new KafkaExport::KafkaAdminClient1C::AdminClient1C();
		}
		return 1;
	}
	return 0;
}
//---------------------------------------------------------------------------//
long DestroyObject(IComponentBase** pIntf)
{ 
	if (!*pIntf) {
		return -1;
	}

	delete *pIntf;
	*pIntf = 0;

	return 0;
}
//---------------------------------------------------------------------------//
const WCHAR_T* GetClassNames()
{
    static char16_t cls_names[] = u"|KafkaProducer|KafkaConsumer|KafkaAdminClient";
    return reinterpret_cast<WCHAR_T *>(cls_names);
}
//---------------------------------------------------------------------------//
AppCapabilities SetPlatformCapabilities(const AppCapabilities capabilities)
{
    return eAppCapabilitiesLast;
}
//---------------------------------------------------------------------------//
AttachType GetAttachType()
{
    return eCanAttachAny;
}
//---------------------------------------------------------------------------//
