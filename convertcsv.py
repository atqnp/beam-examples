 import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
    """docstring for MyOptions."""
    @classmethod
    def _add_argparse_args(cls, parser):
    parser.add_argument('--input',
                        help='Input for the pipeline',
                        default='gs://atiqwork/school_primary')
    parser.add_argument(('--input',
                        help='Output for the pipeline',
                        default='gs://atiqwork/school_primary')
class CSVtoDict(beam.DoFn):
    def process(self,element,headers):
        rec=''
        element = element.encode('utf-8)')
        try:
            for line in csv.reader
            rec = line

            if len(rec) == len(headers):
                data={ header.strip(): val.strip() for header, val in zip(headers,rec)}
                return [data]
            else:
                print "bad: {}".format(rec)
        except Exception:
            pass

with beam.Pipeline(options=PipelineOptions()) as p:
    lines = p | 'ReadFile' >> beam.io.ReadFromText('gs://atiqwork/school_primary/*.csv')
    process = (
        lines
        |
    )
