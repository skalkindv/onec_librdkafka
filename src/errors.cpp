#include "stdafx.h"
#include "component_types.h"

using namespace KafkaExport;


//---------------------------------------------------------------------------//
KafkaExport::ErrorDescription KafkaExport::err(int32_t type, std::string description, bool fatal)
{
	ErrorDescription res;
	
	res.type = type;
	res.description = description;
	res.fatal = fatal;	
	
	return res;
}
//---------------------------------------------------------------------------//
std::string KafkaExport::err_to_str(ErrorDescription err)
{
	std::string res;
	if (err.fatal){
		res = "[Fatal] ";
	}
	
	if (err.type == ERR_EMPTY){
		res = res + err.description;
		return res;
	}
	switch (err.type)
	{
	case ERR_SUCCESS:
		res = res + "Sucess";
		break;
	case ERR_NOTINIT:
		res = res + "Not initialized";
		break;
	case ERR_BADPARAMETR:
		res = res + "Bad parametrs";
		break;
	case ERR_NOTFOUND:
		res = res + "Not found";
		break;
	case ERR_EOF:
		res = res + "EOF";
		break;
	case ERR_SIZELIMIT:
		res = res + "Size limit";
		break;
	case ERR_BADALLOC:
		res = res + "Bad alloc";
		break;
	case ERR_JSONPARSING:
		res = res + "JSON parsing";
		break;
	case ERR_JSONGENERATION:
		res = res + "JSON generation";
		break;
	case ERR_OFFSETSORDER:
		res = res + "Offsets order";
		break;
	case ERR_CONSUMPTION:
		res = res + "Consumption";
		break;	
	case ERR_TIMEOUT:
		res = res + "Timeout";
		break;		
	case ERR_UNHANDLED:
		res = res + "Unhandled";
		break;			
	default:
		res = res + "?Unhandled";
		break;
	}
	if (!err.description.empty()){
		res = res + ": " + err.description;
	}	
	return res;
}
//---------------------------------------------------------------------------//
