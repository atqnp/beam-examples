from __future__ import absolute_import
from __future__ import print_function

import argparse
import logging
import re

from past.builtins import unicode
import requests
import csv
import apache_beam as beam
import jismesh.utils as ju
from beam_utils.sources import CsvFileSource
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

class Print(beam.PTransform):
    def expand(self, pcoll):
        return pcoll | beam.ParDo(print)

class CreateMeshLevel2(beam.DoFn):
    def process(self, element):
    	code = element.get('meshcode')
        list1 = []
        for i in range(8):
            for j in range(8):
                list1.append(str(code)+str(i)+str(j))
        return list1

class PolygonCode(beam.DoFn):
    def process(self, element):
        lat1,lon1 = ju.to_meshpoint(element,0,0)
        lat2,lon2 = ju.to_meshpoint(element,1,1)
        poly_code = 'POLYGON (('+str(lon1)+' '+str(lat1)+','+str(lon1)+' '+str(lat2)+','+str(lon2)+' '+str(lat2)+','+str(lon2)+' '+str(lat1)+','+str(lon1)+' '+str(lat1)+'))'
        return [poly_code]
  
def uploadtogcs(filename):
    path = 'test/' + filename
    client = storage.Client.from_service_account_json('gdacatiq-eb0a18fdefb4.json')
    bucket = client.get_bucket('atiqwork')
    blob = bucket.blob(path)
    blob.upload_from_filename(filename)

with beam.Pipeline(options=PipelineOptions()) as p:
    #readcode = p | 'Read' >> ReadFromText('lv1meshcodelist.csv') | beam.Map(lambda x:(x+str(1))) | Print()
    readcode1 = p | 'Read' >> beam.io.Read(CsvFileSource('lv1meshcodelist.csv')) | beam.ParDo(CreateMeshLevel2()) | Print()
    #readcode2 = p | 'Read' >> ReadFromText('lv1meshcodelist.csv', skip_header_lines=1) | beam.ParDo(CreateMeshLevel2()) | beam.ParDo(PolygonCode())
#'Output' >> WriteToText('test', file_name_suffix='csv')
#create_lv1()
# uploadtogcs("lv1meshcodelist.csv")
# def joincode(code1,code2):
# 	return 
