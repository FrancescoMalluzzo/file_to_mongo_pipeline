import json
from apache_beam import  DoFn
from loguru import logger

class PrepareInput(DoFn):
    """
    Try to parse the json - else give a warning
    """
    def process(self,element):
        try:
            yield json.loads(element)
        except:
            if isinstance(element,dict):
                logger.warning(f"Element {element['_id']} not valid")
            return

class RenameKey(DoFn):
    """
    Rename the key id to _id
    """
    def process(self,element):
        element["_id"] = element.pop("id")
        yield element

class IndexedTuple(DoFn):
    """
    Create and indexed tuple
    """
    def process(self,element,key):
        yield (element.get(key),element)

class RecollectData(DoFn):
    """
    Recollect properly type
    """
    def process(self,element):
        if "_id" in element[-1].get('left')[0].keys():
            yield {**element[-1].get('left')[0],**element[-1].get('right')[0]}
        else:
            logger.warning(f"{element} has no ID")
            return


