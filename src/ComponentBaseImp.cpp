#include "stdafx.h"
#include "ComponentBaseImp.h"


//---------------------------------------------------------------------------//
ComponentBase::ComponentBase(std::u16string Extension_)
{
	m_iMemory = 0;
	m_iConnect = 0;

	Extension = Extension_;
}
//---------------------------------------------------------------------------//
ComponentBase::~ComponentBase()
{
	PropertyFunction.clear();
	PropertyFunction.shrink_to_fit();

	Propertyes.clear();
	Propertyes.shrink_to_fit();
}
//---------------------------------------------------------------------------//
void ComponentBase::AddFunctionProperty(int _countParam, std::string _Name, std::string _NameRu, FuncPtrType  _pFunction, ProcPtrType  _pProcedure)
{
	sPropertyFunction FunctionProperty;

	FunctionProperty.countParam = _countParam;
	FunctionProperty.itFunction = (_pFunction == 0 ? false : true);
	FunctionProperty.pFunction = _pFunction;
	FunctionProperty.pProcedure = _pProcedure;
	FunctionProperty.Name[0] = _Name;
	FunctionProperty.Name[1] = _NameRu;

	PropertyFunction.push_back(FunctionProperty);
}
//---------------------------------------------------------------------------//
void ComponentBase::AddProperty(std::string _Name, std::string _NameRu, bool IsReadeble, bool IsWriteble)
{
	sProperty Property;

	Property.Name[0] = _Name;
	Property.Name[1] = _NameRu;
	Property.IsReadeble = IsReadeble;
	Property.IsWriteble = IsWriteble;

	Propertyes.push_back(Property);
}
//---------------------------------------------------------------------------//
bool ComponentBase::Init(void* pConnection)
{
	m_iConnect = (IAddInDefBase*)pConnection;
	pAsyncEvent = m_iConnect;
	return m_iConnect != nullptr;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetInfo()
{
	// Component should put supported component technology version
	// This component supports 2.0 version
	return 2000;
}
//---------------------------------------------------------------------------//
void ComponentBase::Done()
{

}
//---------------------------------------------------------------------------//
bool ComponentBase::RegisterExtensionAs(WCHAR_T** wsExtensionName)
{
	unsigned int iActualSize = (unsigned int)((Extension.size() + 1) * sizeof(WCHAR_T));

	if (m_iMemory)
	{
		if (m_iMemory->AllocMemory((void**)wsExtensionName, iActualSize)){
			memcpy(*wsExtensionName, Extension.c_str(), iActualSize);
		}
		return true;
	}
	return false;
	
}
//---------------------------------------------------------------------------//
long ComponentBase::GetNProps()
{
	// You may delete next lines and add your own implementation code here
	return (long)Propertyes.size();
}
//---------------------------------------------------------------------------//
long ComponentBase::FindProp(const WCHAR_T* wsPropName)
{
	long ret = -1;
	std::string prop = toUTF8String(wsPropName, strlen16(wsPropName));
	for (uint32_t i = 0; i < Propertyes.size(); i++)
	{
		for (uint32_t j = 0; j < _CountLanguage; j++)
		{
			if (Propertyes[i].Name[j] == prop)
			{
				ret = i;
				break;
			}
		}
	}
	return ret;
}
//---------------------------------------------------------------------------//
const WCHAR_T* ComponentBase::GetPropName(long lPropNum, long lPropAlias)
{
	if (lPropNum >= (long)Propertyes.size())
		return nullptr;

	if (lPropAlias >= _CountLanguage)
		return 0;

	WCHAR_T* wsPropName = nullptr;
	size_t iActualSize = Propertyes[lPropNum].Name[lPropAlias].size();
	unsigned long alloc_size = (unsigned long)((iActualSize + 1) * sizeof(WCHAR_T));

	if (m_iMemory)
	{
		if (m_iMemory->AllocMemory((void**)&wsPropName, alloc_size)) {
			storeUTF8toUTF16LE(Propertyes[lPropNum].Name[lPropAlias].c_str(), iActualSize, wsPropName, iActualSize + 1);
		}
	}

	return wsPropName;
}
//---------------------------------------------------------------------------//
bool ComponentBase::GetPropVal(const long lPropNum, tVariant* pvarPropVal)
{
	return false;
}
//---------------------------------------------------------------------------//
bool ComponentBase::SetPropVal(const long lPropNum, tVariant* varPropVal)
{
	return false;
}
//---------------------------------------------------------------------------//
bool ComponentBase::IsPropReadable(const long lPropNum)
{
	return Propertyes[lPropNum].IsReadeble;
}
//---------------------------------------------------------------------------//
bool ComponentBase::IsPropWritable(const long lPropNum)
{
	return Propertyes[lPropNum].IsWriteble;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetNMethods()
{
	return (long)PropertyFunction.size();
}
//---------------------------------------------------------------------------//
long ComponentBase::FindMethod(const WCHAR_T* wsMethodName)
{	
	std::string method = toUTF8String(wsMethodName, strlen16(wsMethodName));
	long plMethodNum = -1;
	for (uint32_t i = 0; i < PropertyFunction.size(); i++)
	{
		for (uint32_t j = 0; j < _CountLanguage; j++)
		{
			if (PropertyFunction[i].Name[j] == method)
			{
				plMethodNum = i;
				break;
			}
		}
	}
	return plMethodNum;
}
//---------------------------------------------------------------------------//
const WCHAR_T* ComponentBase::GetMethodName(const long lMethodNum, const long lMethodAlias)
{
	if (lMethodNum >= (long)PropertyFunction.size())
		return NULL;

	if (lMethodAlias >= _CountLanguage)
		return 0;

	WCHAR_T* wsMethodName = nullptr;
	size_t iActualSize = PropertyFunction[lMethodNum].Name[lMethodAlias].size();
	unsigned long alloc_size = (unsigned long)((iActualSize + 1) * sizeof(WCHAR_T));
	if (m_iMemory)
	{
		if (m_iMemory->AllocMemory((void**)&wsMethodName, alloc_size)) {
			storeUTF8toUTF16LE(PropertyFunction[lMethodNum].Name[lMethodAlias].c_str(), iActualSize, wsMethodName, iActualSize + 1);
		}
	}

	return wsMethodName;
}
//---------------------------------------------------------------------------//
long ComponentBase::GetNParams(const long lMethodNum)
{
	if (lMethodNum >= (long)PropertyFunction.size()) return 0;

	return PropertyFunction[lMethodNum].countParam;
}
//---------------------------------------------------------------------------//
bool ComponentBase::GetParamDefValue(const long lMethodNum, const long lParamNum, tVariant* pvarParamDefValue)
{
	TV_VT(pvarParamDefValue) = VTYPE_EMPTY;
	return false;
}
//---------------------------------------------------------------------------//
bool ComponentBase::HasRetVal(const long lMethodNum)
{
	if (lMethodNum >= (long)PropertyFunction.size()) return false;

	return PropertyFunction[lMethodNum].itFunction;
}
//---------------------------------------------------------------------------//
bool ComponentBase::CallAsProc(const long lMethodNum, tVariant* paParams, const long lSizeArray)
{	
	try {
		if (lMethodNum >= (long)PropertyFunction.size()) return false;
		return (PropertyFunction[lMethodNum].pProcedure)(paParams, lSizeArray);
	} catch (...) {
		return false;
	}
}
//---------------------------------------------------------------------------//
bool ComponentBase::CallAsFunc(const long lMethodNum, tVariant* pvarRetValue, tVariant* paParams, const long lSizeArray)
{
	try {
		if (lMethodNum >= (long)PropertyFunction.size()) return false;
		return (PropertyFunction[lMethodNum].pFunction)(pvarRetValue, paParams, lSizeArray);
	} catch (...) {
		return false;
	}
}
//---------------------------------------------------------------------------//
void ComponentBase::SetLocale(const WCHAR_T* loc)
{
#if !defined( __linux__ )
    //_wsetlocale(LC_ALL, (wchar_t*)loc);
#else
    //We convert in char* char_locale
    //also we establish locale
    //setlocale(LC_ALL, char_locale);
#endif
}
//---------------------------------------------------------------------------//
bool ComponentBase::setMemManager(void* mem)
{
	m_iMemory = (IMemoryManager*)mem;
	return m_iMemory != 0;
}
//---------------------------------------------------------------------------//
void ADDIN_API ComponentBase::SetUserInterfaceLanguageCode(const WCHAR_T* lang)
{
	return;
}
//---------------------------------------------------------------------------//
void ComponentBase::addError(uint32_t wcode, std::string source,
	std::string descriptor, long code)
{
	if (m_iConnect)
	{
		m_iConnect->AddError(wcode, toUTF16String(source.c_str(), source.size()).c_str(), 
			toUTF16String(descriptor.c_str(),source.size()).c_str(), code);
	}
}
//---------------------------------------------------------------------------//
size_t ComponentBase::strlen16(const WCHAR_T* Source)
{
	return std::char_traits<WCHAR_T>::length(Source);
}
//---------------------------------------------------------------------------//
std::u16string ComponentBase::toUTF16String(const char *src, size_t len)
{
	std::u16string res;
	size_t dest_len = (len + 1);
	WCHAR_T *dst = new WCHAR_T[dest_len];
	storeUTF8toUTF16LE(src, len, dst, dest_len);
	res = std::u16string((char16_t*)dst);
	delete[] dst;
	return res;
}
//---------------------------------------------------------------------------//
std::string ComponentBase::toUTF8String(const WCHAR_T *src, size_t len)
{
	std::string res;
	size_t dest_bytes = (len + 1) * sizeof(WCHAR_T);
	char *dst = new char[dest_bytes];
	storeUTF16LEtoUTF8(src, len, dst, dest_bytes);
	res = std::string(dst);	
	delete[] dst;
	return res;
}
//---------------------------------------------------------------------------//
bool ComponentBase::storeUTF8toUTF16LE(const char *src, size_t src_len, WCHAR_T *dst, size_t dst_len)
{
	size_t dst_len_bytes = dst_len * sizeof(WCHAR_T);
	memset(dst, 0, dst_len_bytes);
#if defined( __linux__ )
	iconv_t conv = iconv_open("UTF-16LE", "UTF-8");
	if (conv == (iconv_t)-1)
		return false; 	  
	char *_src = (char*)src;
	char *_dst = (char*)dst;
	size_t succeed = iconv(conv, &_src, &src_len, &_dst, &dst_len_bytes);
	iconv_close(conv);  
	if(succeed == (size_t)-1)
		return false;
#else
	if (!MultiByteToWideChar(CP_UTF8, 0, src, (uint32_t)src_len, (LPWSTR)dst, (uint32_t)dst_len))
		return false;	
#endif
    return true;
}
//---------------------------------------------------------------------------//
bool ComponentBase::storeUTF16LEtoUTF8(const WCHAR_T *src, size_t src_len, char *dst, size_t dst_len)
{
	memset(dst, 0, dst_len);
#if defined( __linux__ )
	iconv_t conv = iconv_open("UTF-8", "UTF-16LE");
	if (conv == (iconv_t)-1)
		return false;  
	size_t src_len_bytes = src_len * sizeof(WCHAR_T);
	char *_src = (char*)src;
	size_t succeed = iconv(conv, &_src, &src_len_bytes, &dst, &dst_len);
	iconv_close(conv);  
	if(succeed == (size_t)-1)
		return false;
#else
	if (!WideCharToMultiByte(CP_UTF8, 0, (LPWSTR)src, (uint32_t)src_len, dst, (uint32_t)dst_len, NULL, NULL))
		return false;		
#endif
    return true;
}
//---------------------------------------------------------------------------//
void ComponentBase::allocString(tVariant* pvarPropVal, const char* data, size_t src_len)
{

    size_t dst_len = (src_len + 1); 
	if (!m_iMemory->AllocMemory((void**)&pvarPropVal->pwstrVal, (unsigned long)dst_len * 2)) {
		return;
	}
	
	if (storeUTF8toUTF16LE((const char*)data, src_len, pvarPropVal->pwstrVal, dst_len))
	{
		TV_VT(pvarPropVal) = VTYPE_PWSTR;
		pvarPropVal->wstrLen = (uint32_t)strlen16((WCHAR_T*)pvarPropVal->pwstrVal);
	}
}
//---------------------------------------------------------------------------//
void ComponentBase::allocBlob(tVariant* pvarPropVal, char* byte_ptr, unsigned int len)
{
	if (!m_iMemory){
		throw std::bad_alloc();
	}

	if (byte_ptr != nullptr){
		if (len > 0){

			if (m_iMemory->AllocMemory((void**)&pvarPropVal->pstrVal, len)){
				TV_VT(pvarPropVal) = VTYPE_BLOB;
				memcpy(pvarPropVal->pstrVal, byte_ptr, len);
				pvarPropVal->strLen = len;
			}
		}
		else{
			pvarPropVal->strLen = 0;
		}
	}
}
//---------------------------------------------------------------------------//
bool ComponentBase::tVariantIsNumber(tVariant* pvarPropVal)
{
	bool res = false;
	if (((pvarPropVal)->vt == VTYPE_UI4) || ((pvarPropVal)->vt == VTYPE_INT) || ((pvarPropVal)->vt == VTYPE_I1) | ((pvarPropVal)->vt == VTYPE_UI1) || ((pvarPropVal)->vt == VTYPE_UI2)
		|| ((pvarPropVal)->vt == VTYPE_I8) || ((pvarPropVal)->vt == VTYPE_UI8) || ((pvarPropVal)->vt == VTYPE_UINT) || ((pvarPropVal)->vt == VTYPE_I2) || ((pvarPropVal)->vt == VTYPE_I4)
		|| ((pvarPropVal)->vt == VTYPE_R4) || ((pvarPropVal)->vt == VTYPE_R8)) {
		res = true;
	}
	return res;
}

//---------------------------------------------------------------------------//


