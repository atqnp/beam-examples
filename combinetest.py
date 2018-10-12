from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class Print(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(print)

pc = [1,10,100,1000]

def bounded_sum(values,bound=500):
    return min(sum(values),bound)

small_sum = pc | beam.CombineGlobally(bounded_sum) | Print()
large_sum = pc | beam.CombineGlobally(bounded_sum, bound=2000) | Print()
