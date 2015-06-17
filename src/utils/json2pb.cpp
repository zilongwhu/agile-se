#include "utils/json2pb.h"
#include <google/protobuf/descriptor.h>

using namespace google::protobuf;

static bool _json2pb(Message &msg, cJSON *json);
static bool _json2field(Message &msg, const FieldDescriptor *field, cJSON *json)
{
    const Reflection *ref = msg.GetReflection();
    const bool repeated = field->is_repeated();

    switch(field->cpp_type())
    {
        case FieldDescriptor::CPPTYPE_INT32:
            if (cJSON_Number != json->type)
            {
                P_WARNING("[%s] pb is int32, json is not a number", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddInt32(&msg, field, (int32_t)json->valuedouble);
            else
                ref->SetInt32(&msg, field, (int32_t)json->valuedouble);
            break;
        case FieldDescriptor::CPPTYPE_INT64:
            if (cJSON_Number != json->type)
            {
                P_WARNING("[%s] pb is int64, json is not a number", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddInt64(&msg, field, (int64_t)json->valuedouble);
            else
                ref->SetInt64(&msg, field, (int64_t)json->valuedouble);
            break;
        case FieldDescriptor::CPPTYPE_UINT32:
            if (cJSON_Number != json->type)
            {
                P_WARNING("[%s] pb is uint32, json is not a number", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddUInt32(&msg, field, (uint32_t)json->valuedouble);
            else
                ref->SetUInt32(&msg, field, (uint32_t)json->valuedouble);
            break;
        case FieldDescriptor::CPPTYPE_UINT64:
            if (cJSON_Number != json->type)
            {
                P_WARNING("[%s] pb is uint64, json is not a number", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddUInt64(&msg, field, (uint64_t)json->valuedouble);
            else
                ref->SetUInt64(&msg, field, (uint64_t)json->valuedouble);
            break;
        case FieldDescriptor::CPPTYPE_DOUBLE:
            if (cJSON_Number != json->type)
            {
                P_WARNING("[%s] pb is double, json is not a number", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddDouble(&msg, field, json->valuedouble);
            else
                ref->SetDouble(&msg, field, json->valuedouble);
            break;
        case FieldDescriptor::CPPTYPE_FLOAT:
            if (cJSON_Number != json->type)
            {
                P_WARNING("[%s] pb is float, json is not a number", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddFloat(&msg, field, json->valuedouble);
            else
                ref->SetFloat(&msg, field, json->valuedouble);
            break;
        case FieldDescriptor::CPPTYPE_BOOL:
            if (cJSON_True != json->type && cJSON_False != json->type)
            {
                P_WARNING("[%s] pb is bool, json is not", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddBool(&msg, field, json->type == cJSON_True);
            else
                ref->SetBool(&msg, field, json->type == cJSON_True);
            break;
        case FieldDescriptor::CPPTYPE_ENUM:
            {
                const EnumDescriptor *ed = field->enum_type();
                const EnumValueDescriptor *ev = NULL;
                if (NULL == ed)
                {
                    P_WARNING("failed to get enum descriptor");
                    return false;
                }
                if (cJSON_Number == json->type)
                {
                    ev = ed->FindValueByNumber(json->valueint);
                    if (NULL == ev)
                    {
                        P_WARNING("failed to find enum by value: %d", json->valueint);
                        return false;
                    }
                }
                else if (cJSON_String == json->type)
                {
                    ev = ed->FindValueByName(json->valuestring);
                    if (NULL == ev)
                    {
                        P_WARNING("failed to find enum by name: %s", json->valuestring);
                        return false;
                    }
                }
                else
                {
                    P_WARNING("[%s] pb is enum, json is not", field->name().c_str());
                    return false;
                }
                if (repeated)
                    ref->AddEnum(&msg, field, ev);
                else
                    ref->SetEnum(&msg, field, ev);
            }
            break;
        case FieldDescriptor::CPPTYPE_STRING:
            if (cJSON_String != json->type)
            {
                P_WARNING("[%s] pb is string, json is not", field->name().c_str());
                return false;
            }
            if (repeated)
                ref->AddString(&msg, field, json->valuestring);
            else
                ref->SetString(&msg, field, json->valuestring);
            break;
        case FieldDescriptor::CPPTYPE_MESSAGE:
            {
                Message *mf = repeated ? ref->AddMessage(&msg, field) : ref->MutableMessage(&msg, field);
                if (NULL == mf)
                {
                    P_WARNING("failed to get message");
                    return false;
                }
                return _json2pb(*mf, json);
            }
    }
    return true;
}

static bool _json2pb(Message &msg, cJSON *json)
{
    const Descriptor *des = msg.GetDescriptor();
    const Reflection *ref = msg.GetReflection();
    if (NULL == des || NULL == ref)
    {
        P_WARNING("des[%p] or ref[%p] is NULL", des, ref);
        return false;
    }
    for (cJSON *chld = json->child; NULL != chld; chld = chld->next)
    {
        if (NULL == chld->string)
        {
            P_WARNING("sub item does not have name");
            return false;
        }
        const FieldDescriptor *field = des->FindFieldByName(chld->string);
        if (NULL == field)
        {
            field = ref->FindKnownExtensionByName(chld->string);
        }
        if (NULL == field)
        {
            P_WARNING("failed to get field by name[%s]", chld->string);
            return false;
        }
        if (field->is_repeated())
        {
            if (cJSON_Array != chld->type)
            {
                P_WARNING("pb field[%s] is array, but json is not", chld->string);
                return false;
            }
            for (cJSON *cc = chld->child; NULL != cc; cc = cc->next)
            {
                if (!_json2field(msg, field, cc))
                {
                    return false;
                }
            }
        }
        else
        {
            if (!_json2field(msg, field, chld))
            {
                return false;
            }
        }
    }
    return true;
}

bool json2pb(Message &msg, cJSON *json)
{
    if (NULL == json)
    {
        return false;
    }
    if (cJSON_Object != json->type)
    {
        return false;
    }
    return _json2pb(msg, json);
}
