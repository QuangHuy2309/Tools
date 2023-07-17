import json
import os
import csv


class AvroHandler:
    def __init__(self, path, publisher) -> None:
        self.path = path
        self.outfile = ""
        self.publisher = publisher
        self.fields = []

    def read_json(self):
        with open(self.path) as f:
            self.data = json.load(f)

    def write_json(self):
        file_name = os.path.splitext(self.path)[0]

        final_result = {
            'namespace': f'com.unified.avroschemas.{self.publisher.lower()}.{file_name}',
            'type': 'record',
            'doc': f'This Schema describes about {self.publisher} {file_name}',
            'name': f'{self.publisher}{file_name[:1].upper()}{file_name[1:]}',
            'fields': self.fields
        }

        os.makedirs('result_optimize', exist_ok=True)
        self.outfile = f'./result_optimize/{file_name}.avsc'
        with open(self.outfile, 'w') as outfile:
            json.dump(final_result, outfile)

    def convert_key(self, key):
        return ''.join('_' + c.lower() if c.isupper() else c for c in key)

    def convert_name(self, key):
        return ''.join(k.capitalize() for k in key.split('_'))

    def get_data_type(self, _data):
        data_type_map = {
            bool: 'boolean',
            str: 'string',
            int: 'int',
            float: 'double',
            type(None): '// TODO: double check data type here.'
        }
        return ['null', data_type_map.get(type(_data))]

    def get_data_type_array(self, _data):
        return ['null', {'type': 'array', 'items': data_type_map.get(type(_data[0]))}]

    def get_data_type_array_with_record(self, field_name, _data):
        items = {
            'name': self.convert_name(field_name),
            'type': 'record',
            'fields': [{'name': k, 'type': self.get_data_type(v)} for k, v in _data.items()]
        }
        return {'type': 'array', 'items': items}

    def generate_dict_for_field(self, field_name, nested_data):
        _data = nested_data.get(field_name)
        if isinstance(_data, (str, int, float)) or _data is None:
            return {
                'name': field_name,
                'type': self.get_data_type(_data)
            }

        elif isinstance(_data, dict):
            return {
                'name': field_name,
                'type': {
                    'type': 'record',
                    'name': self.convert_name(field_name),
                    'fields': [self.generate_dict_for_field(k, _data) for k in _data]
                }
            }

        elif isinstance(_data, list):
            if isinstance(_data[0], (str, int, float)):
                return {
                    'name': field_name,
                    'type': self.get_data_type_array(_data)
                }
            else:
                return {
                    'name': field_name,
                    'type': self.get_data_type_array_with_record(field_name, _data[0])
                }

    def generate_avro(self):
        self.read_json()
        self.fields = [self.generate_dict_for_field(k, self.data) for k in self.data]
        self.write_json()

    def get_record_type(self, prefix, _data):
        list_record = []
        for d in _data:
            if isinstance(d.get('type'), list):
                list_record.append([
                    prefix + d.get('name'),
                    self.convert_key(d.get('name')),
                    d.get('type')[1] if isinstance(d.get('type')[1], dict) else d.get('type')[1]
                ])
            else:
                new_dict = d.get('type')
                if new_dict.get('type') == 'record':
                    list_record.extend(self.get_record_type(prefix + d.get('name') + '-', new_dict.get('fields')))
                else:
                    fields = new_dict.get('items')
                    list_record.extend(self.get_record_type(prefix + d.get('name') + '-', fields.get('fields')))
        return list_record

    def get_key_and_data_type(self):
        list_record = self.get_record_type('', self.fields)
        header = ['avro_field_name', 'transformed_name', 'data_type']
        with open('result_optimize/check.csv', 'w', newline='\n') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(list_record)


class TransformHandler:
    def __init__(self, publisher, prefix, name):
        self.path = 'result_optimize/check.csv'
        self.data = []
        self.fields = []
        self.publisher = publisher
        self.prefix = prefix
        self.name = name
        self.result_path = f'result_optimize/transform_{self.prefix}.avsc'

    def write_result(self):
        final_result = {
            'namespace': f'com.unified.avroschemas.{self.publisher.lower()}.{self.prefix}transformed',
            'type': 'record',
            'doc': f'This schema describes about {self.publisher} {self.prefix} Transformed',
            'name': self.name,
            'fields': self.fields
        }
        with open(self.result_path, 'w') as f:
            json.dump(final_result, f)

    def pre_process(self):
        with open(self.path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                avro_name = row.get('avro_field_name')
                if avro_name.startswith('payload-'):
                    row['avro_field_name'] = avro_name.replace('payload-', '')
                else:
                    row['avro_field_name'] = avro_name.replace('metaData-', '')

                self.data.append(row)

    def get_type(self, data_type):
        type_map = {
            'long': {'type': 'long', 'logicalType': 'timestamp-millis'},
            'string': {'type': 'string', 'avro.java.string': 'String'}
        }
        return ['null', type_map.get(data_type, data_type)]

    def process(self):
        self.pre_process()

        for d in self.data:
            transformed_name = d.get('transformed_name')
            data_type = d.get('data_type')
            if '-' in d.get('avro_field_name'):
                continue
            temp_dict = {
                'name': transformed_name,
                'type': self.get_type(data_type)
            }
            self.fields.append(temp_dict)

        self.write_result()


runner = AvroHandler('profile.json', 'Xandr')
runner.generate_avro()
runner.get_key_and_data_type()

transform = TransformHandler('Xandr', 'profile', 'XandrProfileTransformed')
transform.process()
