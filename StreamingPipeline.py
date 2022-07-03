import argparse
import logging
import sys
import datetime
import random
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import DoFn, GroupByKey, ParDo, PTransform, WindowInto, WithKeys
from apache_beam.transforms.window import FixedWindows
from bigquery_schema_generator.generate_schema import SchemaGenerator, read_existing_schema_from_file
from google.cloud import bigquery

parser = argparse.ArgumentParser()


#Define custom Pipeline Options

class JobOptions(PipelineOptions):
    @classmethod
    def add_argparse_args(cls, parse) -> None:
        parser.add_argument(
            '--project_id',
            type = str,
            help = 'Project ID of the GCP project',
            default = None
        )

        parser.add_argument(
            '--input_subscription',
            type = str,
            help = 'Topic from PubSub.\n'
        )

        parser.add_argument(
            '--bq_window_size',
            type = int,
            help = 'Window error file in minutes'
        )

        parser.add_argument(
            '--bigquery_dataset',
            type = str,
            help = 'BigQuery Dataset to write streaming data'
        )

        parser.add_argument(
            "--bigquery_table",
            type = str
            help = 'bigquery table to write streaming data'
        )


class GroupMessagesByFixedWindows(PTransform):

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )

class ModifyFailedRows(beam.DoFn):
    def __init__(self, bq_dataset: str, bq_table: str) -> None:
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
    
    def start_bundle(self):
        self.client = bigquery.Client()

    def process(self, batch):
        logging.info(f'{len(batch)} failed rows')
        table_id = f'{self.bq_dataset}.{self.bq_table}'

        #Deduce new cchema from the original schema
        #If the table does not exist then proceed with empty schema

        try:
            table_file_name = f'original_schema_{self.bq_table}.json'
            table = self.client.get_table(table_id)
            self.client.schema_to_json(table.schema, table_file_name)
            original_schema = read_existing_schema_from_file(table_file_name)

        except Exception:
            logging.info(f'{table_id} table does not exist. Proceed without schema')
            original_schema = {}

        
        #generate new schema

        schema_map = SchemaGenerator.deduce_schema(input_data= batch, schema_map= original_schema)
        schema = SchemaGenerator.flatten_schema(schema_map)     
 
        job_config = bigquery.LoadJobConfig(
            source_format=
            bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            write_disposition=
            bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema
        )

        try:
            load_job = self.client.load_table_from_json(
                batch,
                table_id,
                job_config=job_config,
            )   
            
            
            load_job.result() 
            
            if load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")
            else:
                logging.info(f'Loaded {len(batch)} rows.')

        except Exception as error:
            logging.info(f'Error: {error} with loading dataframe')

def run(argv):
    parser = argparse.ArgumentParser()
    pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options = pipeline_options.view_as(JobOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        streaming_data = (
                pipeline
                | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=options.input_subscription, with_attributes=False)
                #Attributes set to false to avoid further processing
                | f"Write to {options.bq_table}" >> beam.io.WriteToBigQuery(
                    table=f"{options.bq_dataset}.{options.bq_table}",
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    insert_retry_strategy=beam.io.gcp.bigquery_tools.RetryStrategy.RETRY_NEVER
                )
        )

        (
            streaming_data[beam.io.gcp.bigquery.BigQueryWriteFn.FAILED_ROWS]
            | f"Window Into" >> GroupMessagesByFixedWindows(window_size=options.bq_window_size)
            | f"Failed Rows for {options.bq_table}" >> beam.ParDo(ModifyFailedRows(options.bq_dataset, options.bq_table))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
