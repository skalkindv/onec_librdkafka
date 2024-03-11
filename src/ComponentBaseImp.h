#ifndef __COMPONENTBASEIMP_H__
#define __COMPONENTBASEIMP_H__

#include <functional>
#include <string>
#include <vector>
#include <string_view>
#include "ComponentBase.h"
#include "AddInDefBase.h"
#include "IMemoryManager.h"

#if defined( __linux__ )
#include <iconv.h>
#endif

#if !defined( __linux__ )
#include <windows.h>
#endif


const int _CountLanguage = 2;

class ComponentBase : public IComponentBase
{

public:

	typedef std::function<bool(tVariant*, tVariant*, const long)> FuncPtrType;
	typedef std::function<bool(tVariant*, const long)> ProcPtrType;

    struct sPropertyFunction
	{
		int countParam = 0;
		bool itFunction = false;
		std::string Name[_CountLanguage];
		FuncPtrType  pFunction;
		ProcPtrType  pProcedure;
	};

	struct sProperty
	{
		std::string Name[_CountLanguage];
		bool IsReadeble = false;
		bool IsWriteble = false;
	};
	

	
	ComponentBase(std::u16string Extension_);
	virtual ~ComponentBase();
	// IInitDoneBase
	virtual bool ADDIN_API Init(void*);
	virtual bool ADDIN_API setMemManager(void* mem);
	virtual long ADDIN_API GetInfo();
	virtual void ADDIN_API Done();
	// ILanguageExtenderBase
	virtual bool ADDIN_API RegisterExtensionAs(WCHAR_T**);
	virtual long ADDIN_API GetNProps();
	virtual long ADDIN_API FindProp(const WCHAR_T* wsPropName);
	virtual const WCHAR_T* ADDIN_API GetPropName(long lPropNum, long lPropAlias);
	virtual bool ADDIN_API GetPropVal(const long lPropNum, tVariant* pvarPropVal);
	virtual bool ADDIN_API SetPropVal(const long lPropNum, tVariant* varPropVal);
	virtual bool ADDIN_API IsPropReadable(const long lPropNum);
	virtual bool ADDIN_API IsPropWritable(const long lPropNum);
	virtual long ADDIN_API GetNMethods();
	virtual long ADDIN_API FindMethod(const WCHAR_T* wsMethodName);
	virtual const WCHAR_T* ADDIN_API GetMethodName(const long lMethodNum, const long lMethodAlias);
	virtual long ADDIN_API GetNParams(const long lMethodNum);
	virtual bool ADDIN_API GetParamDefValue(const long lMethodNum, const long lParamNum, tVariant* pvarParamDefValue);
	virtual bool ADDIN_API HasRetVal(const long lMethodNum);
	virtual bool ADDIN_API CallAsProc(const long lMethodNum, tVariant* paParams, const long lSizeArray);
	virtual bool ADDIN_API CallAsFunc(const long lMethodNum, tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray);
	// LocaleBase
	virtual void ADDIN_API SetLocale(const WCHAR_T* loc);
	// UserLanguageBase
    virtual void ADDIN_API SetUserInterfaceLanguageCode(const WCHAR_T* lang) override;

protected:

	IAddInDefBase* m_iConnect = nullptr;
	IMemoryManager* m_iMemory = nullptr;
	IAddInDefBase* pAsyncEvent = nullptr;

	std::vector<sPropertyFunction> PropertyFunction;
	std::vector<sProperty> Propertyes;
	std::u16string Extension;

	long findName(std::wstring name) const;
	void allocString(tVariant* pvarPropVal, const char* data, size_t src_len);
	void allocBlob(tVariant* pvarPropVal, char *byte_ptr, unsigned int len);
	void addError(uint32_t wcode, std::string source, std::string descriptor, long code);
	void AddFunctionProperty(int _countParam, std::string _Name, std::string _NameRu, FuncPtrType _pFunction, ProcPtrType _pProcedure);
	void AddProperty(std::string _Name, std::string _NameRu, bool IsReadeble, bool IsWriteble);
	bool tVariantIsNumber(tVariant* pvarPropVal);
	size_t strlen16(const WCHAR_T* Source);
	bool storeUTF8toUTF16LE(const char *src, size_t src_len, WCHAR_T *dst, size_t dst_len);
	bool storeUTF16LEtoUTF8(const WCHAR_T *src, size_t src_len, char *dst, size_t dst_len);
   	std::string toUTF8String(const WCHAR_T *src, size_t len);
	std::u16string toUTF16String(const char *src, size_t len);
};

#endif
