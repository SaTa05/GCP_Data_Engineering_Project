import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.util import BatchElements
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io import ReadFromText, WriteToParquet
import apache_beam.transforms.window as window
from apache_beam.coders import RowCoder
from datetime import datetime, timedelta
import json
import csv
from io import StringIO
from google.cloud import bigquery  # For updating state table
import logging

# =====================================================
# Custom DoFns
# =====================================================
class ParseBronzeFile(beam.DoFn):
    """Parse CSV or JSON lines from GCS files."""
    def __init__(self, file_type, delimiter=',', header=True):
        self.file_type = file_type
        self.delimiter = delimiter
        self.header = header
        self.headers = None

    def process(self, element):
        line = element.strip()
        if not line:
            return

        if self.file_type == 'csv':
            reader = csv.reader(StringIO(line), delimiter=self.delimiter)
            row = next(reader)
            if self.header and self.headers is None:
                self.headers = row
                return
            if self.header:
                yield dict(zip(self.headers, row))
            else:
                yield row  # list if no header

        elif self.file_type == 'json':
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON: {line}")

class ApplySchemaAndExtra(beam.DoFn):
    """Apply SchemaConfig casts + ExtraConfig rules using side inputs."""
    def process(self, element, schema_side, extra_side):
        row = element

        # Apply schema casts
        for schema_row in schema_side:
            col = schema_row['column_name']
            if col in row:
                dtype = schema_row['data_type']
                try:
                    if dtype.startswith('int') or dtype == 'integer':
                        row[col] = int(row[col]) if row[col] else None
                    elif dtype.startswith('float') or dtype == 'double':
                        row[col] = float(row[col]) if row[col] else None
                    elif dtype == 'timestamp' or dtype == 'datetime':
                        row[col] = datetime.fromisoformat(row[col]) if row[col] else None
                    # Add more types as needed
                except (ValueError, TypeError):
                    logging.warning(f"Cast failed for {col}: {row[col]}")

        # Apply extra config (filter/transform)
        skip = False
        for extra in extra_side:
            if extra['config_type'] == 'filter':
                # Simple expr eval - be careful with security in production
                if not eval(extra['config_value'], {"row": row}):  # Dangerous - use safer parser
                    skip = True
                    break
            elif extra['config_type'] == 'transform':
                try:
                    row[extra['target_column']] = eval(extra['config_value'], {"row": row})
                except:
                    pass

        if not skip:
            yield row

class DeduplicateLatest(beam.DoFn):
    """After GroupByKey, keep row with latest InvoiceDate."""
    def process(self, grouped):
        key, values = grouped
        if not values:
            return

        latest = max(values, key=lambda x: x.get('InvoiceDate', datetime.min))
        yield latest

class UpdateState(beam.DoFn):
    """Final DoFn: update IngestionState in BigQuery."""
    def __init__(self, project_id):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def process(self, element, provider_id):
        # element = processed row
        max_event_time = element.get('InvoiceDate')
        if not max_event_time:
            return

        query = f"""
        UPDATE config_metadata.IngestionState
        SET
          last_processed_time = CURRENT_TIMESTAMP(),
          max_event_time_seen = TIMESTAMP('{max_event_time.isoformat()}'),
          last_run_status = 'SUCCESS',
          last_run_time = CURRENT_TIMESTAMP(),
          updated_at = CURRENT_TIMESTAMP()
        WHERE provider_id = '{provider_id}'
          AND layer = 'silver'
        """
        try:
            self.client.query(query).result()
            logging.info(f"Updated state for provider {provider_id}")
        except Exception as e:
            logging.error(f"State update failed: {e}")

# =====================================================
# Main Pipeline
# =====================================================
def run_pipeline(argv=None):
    options = PipelineOptions(
        flags=argv,
        runner='DataflowRunner',  # or 'DirectRunner' for local test
        project='playground-s-11-60ee7ccc',  # your sandbox project
        region='us-east1',  # adjust
        temp_location='gs://your-temp-bucket/temp/',
        staging_location='gs://your-temp-bucket/staging/',
        experiments=['use_runner_v2'],  # optional, better perf
    )

    with beam.Pipeline(options=options) as p:
        # Read small config tables (as side inputs)
        data_config_pcoll = (
            p | 'Read DataConfig' >> ReadFromBigQuery(
                query="SELECT * FROM config_metadata.DataConfig WHERE active = TRUE",
                use_standard_sql=True
            )
        )

        schema_config_pcoll = (
            p | 'Read SchemaConfig' >> ReadFromBigQuery(
                query="SELECT * FROM config_metadata.SchemaConfig WHERE active = TRUE",
                use_standard_sql=True
            )
        )

        extra_config_pcoll = (
            p | 'Read ExtraConfig' >> ReadFromBigQuery(
                query="SELECT * FROM config_metadata.ExtraConfig",
                use_standard_sql=True
            )
        )

        state_pcoll = (
            p | 'Read IngestionState' >> ReadFromBigQuery(
                query="SELECT * FROM config_metadata.IngestionState WHERE layer = 'silver'",
                use_standard_sql=True
            )
        )

        # Collect configs as side inputs (small data)
        data_config_view = beam.pvalue.AsList(data_config_pcoll)
        schema_view = beam.pvalue.AsList(schema_config_pcoll)
        extra_view = beam.pvalue.AsList(extra_config_pcoll)
        state_view = beam.pvalue.AsList(state_pcoll)

        # Determine providers to process (small, can collect in memory on driver)
        def get_providers_to_process(data_configs, state_rows):
            now = datetime.utcnow()
            providers = []
            state_dict = {r['provider_id']: r for r in state_rows}
            freq_map = {"hourly": 60, "daily": 1440, "weekly": 10080}

            for cfg in data_configs:
                pid = cfg['provider_id']
                freq_min = freq_map.get(cfg.get('ingestion_frequency', 'daily'), 1440)
                last_time = state_dict.get(pid, {}).get('last_processed_time')
                if last_time is None or (now - last_time) >= timedelta(minutes=freq_min):
                    providers.append((pid, cfg, state_dict.get(pid, {})))
            return providers

        providers = (
            p | 'Create Impulse' >> beam.Create([None])
              | 'Compute Providers' >> beam.FlatMap(
                  lambda _: get_providers_to_process(
                      beam.pvalue.AsList(data_config_pcoll),
                      beam.pvalue.AsList(state_pcoll)
                  )
              )
        )

        # For each provider, read bronze, process, write
        processed = (
            providers
            | 'Process Per Provider' >> beam.ParDo(
                ProcessProviderDoFn(),  # Define below or inline
                data_view=data_config_view,
                schema_view=schema_view,
                extra_view=extra_view,
                state_view=state_view
            )
            | 'Group by InvoiceNo' >> beam.GroupByKey()
            | 'Dedup Latest' >> beam.ParDo(DeduplicateLatest())
            | 'Write Silver Parquet' >> WriteToParquet(
                file_path_prefix='gs://your-silver-bucket/silver/',
                file_name_suffix='.parquet',
                # schema=... (infer or define RowCoder schema)
            )
            | 'Update State' >> beam.ParDo(UpdateState(options.view_as(PipelineOptions).project))
        )

# You need a custom DoFn for per-provider processing (read bronze, filter incremental, apply rules)
class ProcessProviderDoFn(beam.DoFn):
    def process(self, element, data_view, schema_view, extra_view, state_view):
        provider_id, data_cfg, state = element
        bronze_path = data_cfg['bronze_path']
        # lookback_minutes = state.get('lookback_minutes', 0)
        # effective_start = ... compute

        # Read bronze files (assume wildcard or single path for simplicity)
        lines = yield beam.pvalue.pvalue.PCollection.from_underlying(
            ReadFromText(bronze_path + '/*.csv' if data_cfg['file_type'] == 'csv' else bronze_path + '/*.json')
        )

        parsed = (
            lines
            | f'Parse Bronze {provider_id}' >> beam.ParDo(ParseBronzeFile(
                data_cfg['file_type'],
                data_cfg['delimiter'],
                data_cfg['header']
            ))
            | f'Apply Schema/Extra {provider_id}' >> beam.ParDo(
                ApplySchemaAndExtra(),
                schema_side=schema_view,
                extra_side=extra_view
            )
            # Add incremental filter if needed
        )

        for row in parsed:
            yield (row.get('InvoiceNo'), row)  # for later GroupByKey

# Run the pipeline
if __name__ == '__main__':
    import sys
    run_pipeline(sys.argv)