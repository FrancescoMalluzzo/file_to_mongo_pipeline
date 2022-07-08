import os
import sys
import traceback
import json
from apache_beam import  DoFn,ParDo
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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



with beam.Pipeline(options=PipelineOptions()) as p:
    try:
        logger.info("Starting pipeline")
        input_collection = (
                p
                | 'Read from jsonl files' >> beam.io.ReadFromText(os.path.join("input","input*.jsonl"))
                | 'Prepare input' >> ParDo(PrepareInput())
                )

        transformed_collection = (
                input_collection
                | 'String cleaning' >> beam.Map(lambda d: {k: v.strip() if isinstance(v, str) else v for k, v in d.items()})
                | 'Rename Keys' >> ParDo(RenameKey())
                | 'Index tuple' >> ParDo(IndexedTuple(),key="_id")
                )
        side_collection = (
            p
            | 'Read from side files' >> beam.io.ReadFromText(os.path.join("input","side*.jsonl"))
            | 'Parse side element' >> ParDo(PrepareInput())
            | 'Rename side keys' >> ParDo(RenameKey())
            | 'Index side tuple' >> ParDo(IndexedTuple(),key="_id")
        )
        final_collection=(
             ({'left': transformed_collection, 'right': side_collection} | beam.CoGroupByKey())
             | 'Recollect data' >> ParDo(RecollectData())
        )

        output_collection = (
            final_collection
            | 'WriteToMongoDB' >>  beam.io.WriteToMongoDB(uri='mongodb://admin:admin@localhost:27017/admin',
                            db='output_db',
                            coll='output',
                            batch_size=1000)
        )
        logger.info(f"Finished pipeline")
    except Exception as exc: 
        logger.critical(f"An exception occured \n {exc} \n {traceback.print_exc()}")
        sys.exit(1)


