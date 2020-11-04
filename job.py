# import das bibliotecas para ler json, apache beam, pandas
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
import json
import re
from pandas.io.json import json_normalize
import pandas as pd


# caminho para os diretorios de input e output
INPUT_PATH = './input/*.json'
OUTPUT_PATH = './output/abandoned-carts.json'


options = PipelineOptions()
p = beam.Pipeline(options=options)


# construir pipeline
#pipeline_options = PipelineOptions()
# with beam.Pipeline(options=pipeline_options) as pipeline:
#   lines = (
#        pipeline
#        | ReadFromText(INPUT_PATH))


# class Split(beam.DoFn):
#    def process(self, element):
#        ind = element.split(",")
#        return ind

# Classe para customizar o json
class custom_json_parser(beam.DoFn):
    def process(self, element):
        norm = json_normalize(element, max_level=1)
        l = norm["customer.data"].to_list()
        return l


# Função de leitura do json
def parse_json(data):
    return json.loads(data)


# Classe para printar a PCollection
class Printer(beam.DoFn):
    def process(self, data_item):
        print(data_item)

    def printer(data_item):
        print(data_item)


# Ler o arquivo com a partição customer
def partition_fn(element, num_partitions):
    return (element['customer.data'])


# Classe para ler o timestamp como parametro
class ProcessRecord(beam.DoFn):

    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        # access timestamp of element.
        pass


data = (p
        | "Read text" >> ReadFromText(INPUT_PATH)
        | "Write" >> WriteToText(OUTPUT_PATH, file_name_suffix='.json')
#       | 'pair' >> beam.Map()
#       | 'group' >> beam.GroupByKey()
)

result = p.run()


# ptransform:
# ParDo
# GroupByKey
# CoGroupByKey
# Combine
# Flatten
# Partition

#agrupar por customer e aplicar uma formula para gerar o output apenas se o timestamp passar de 10 minutos parado na pagina basket ou na pagina product         
