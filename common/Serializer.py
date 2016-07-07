import json,logging
logger = logging.getLogger(__name__)


def serialize(obj,sort_keys=False,pretty_print=None):
   return json.dumps(obj,sort_keys=sort_keys,indent=pretty_print)

def deserialize(text):
   #logger.debug(' Deserializing: \n' + text)
   return json.loads(text)

def convert_unicode_string(unicode_string):
   if unicode_string is not None:
      return str(unicode_string)
   return None
