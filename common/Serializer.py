import json,logging,datetime
logger = logging.getLogger(__name__)


def serialize(obj,sort_keys=False,pretty_print=None,use_datetime=True):
   if use_datetime:
      return json.dumps(obj,sort_keys=sort_keys,indent=pretty_print,cls=DateTimeEncoder)
   return json.dumps(obj,sort_keys=sort_keys,indent=pretty_print)

def deserialize(text):
   #logger.debug(' Deserializing: \n' + text)
   return json.loads(text)

def convert_unicode_string(unicode_string):
   if unicode_string is not None:
      return str(unicode_string)
   return None

class DateTimeEncoder(json.JSONEncoder):
     def default(self, obj):
         if isinstance(obj, datetime.datetime):
             return obj.strftime("%Y-%m-%d %H:%M:%S %z")
         # Let the base class default method raise the TypeError
         return json.JSONEncoder.default(self, obj)