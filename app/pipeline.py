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


with beam.Pipeline(options=PipelineOptions()) as p:
    logger.info("Starting pipeline")
    input_collection = (
            p
            | 'Read from jsonl files' >> beam.io.ReadFromText("./input/*.jsonl")
            | 'Prepare input' >> ParDo(PrepareInput()))
    transformed_collection = (
            input_collection
            | 'String cleaning' >> beam.Map(lambda d: {k: v.strip() if isinstance(v, str) else v for k, v in d.items()})
            | 'Rename Keys' >> ParDo(RenameKey()))
    
    output_collection = (
        transformed_collection
        | 'WriteToMongoDB' >>  beam.io.WriteToMongoDB(uri='mongodb://admin:admin@mongo:27017/admin',
                        db='output_db',
                        coll='output',
                        batch_size=1000)
    )
    logger.info(f"Finished pipeline")


