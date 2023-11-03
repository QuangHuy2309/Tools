import json
import os
import csv


class AvroHandler:
    def __init__(self, path, publisher):
        self.path = path
        self.publisher = publisher
        self.fields = []
        self.file_name = ""

    def read_json(self, file_path):
        with open(file_path, 'r') as file:
            self.data = json.load(file)

    def write_json(self, file_name):
        final_result = {
            'namespace': f'com.unified.avroschemas.{self.publisher.lower()}.{file_name}',
            'type': 'record',
            'doc': f'This Schema describes {self.publisher} {file_name}',
            'name': f'{self.publisher}{file_name[:1].upper()}{file_name[1:]}',
            'fields': self.fields
        }

        os.makedirs('result', exist_ok=True)
        outfile = f'result/{file_name}.avsc'
        with open(outfile, 'w') as file:
            json.dump(final_result, file)

    def convert_key(self, key):
        result = ""
        for x in range(len(key)):
            if key[x].isupper():
                result = result + "_" + key[x].lower()
            else:
                result = result + key[x]

        return result

    def convert_name(self, key):
        arr_key = key.split("_")
        result_key = ""
        for k in arr_key:
            temp = k[0]
            result_key = result_key + temp.upper() + k[1:]

        return result_key

    def get_data_type(self, data):
        data_type = ["null"]
        if isinstance(data, bool):
            data_type.append("boolean")
        elif isinstance(data, str):
            data_type.append("string")
        elif isinstance(data, int):
            data_type.append("int")
        elif isinstance(data, float):
            data_type.append("double")
        elif data is None:
            data_type.append("// TODO: double check data type here.")

        return data_type

    def get_data_type_array(self, data):
        data_type = ["null"]
        array_type = {}
        array_type.update({"type": "array"})
        if isinstance(data, bool):
            array_type.update({"items": "boolean"})
        elif isinstance(data, str):
            array_type.update({"items": "string"})
        elif isinstance(data, int):
            array_type.update({"items": "int"})
        elif isinstance(data, float):
            if data.is_integer():
                data_type.append("long")
                return data_type
            else:
                array_type.update({"items": "double"})
        data_type.append(array_type)

        return data_type

    def get_data_type_array_with_record(self, field_name, data):
        result = {}
        items = {}
        items["name"] = self.convert_name(field_name)
        items["type"] = "record"
        fields = []
        for key in data:
            temp_field = {}
            data_type_list = self.get_data_type(data.get(key))
            temp_field.update({"name": key})
            temp_field.update({"type": data_type_list})

            fields.append(temp_field)

        items.update({"fields": fields})

        result["type"] = "array"
        result["items"] = items

        return result

    def generate_dict_for_field(self, field_name, nested_data):
        result = {}
        data = nested_data.get(field_name)
        if isinstance(data, (str, int, float)) or data is None:
            data_type = self.get_data_type(data)

            result.update({"name": field_name})
            result.update({"type": data_type})

        elif isinstance(data, dict):
            temp = {}
            temp["type"] = "record"
            temp["name"] = self.convert_name(field_name)
            list_type = []
            for key in data:
                list_type.append(self.generate_dict_for_field(key, data))

            temp["fields"] = list_type

            result.update({"name": field_name})
            result.update({"type": temp})

        elif isinstance(data, list):
            if isinstance(data[0], (str, int, float)):
                result.update({"name": field_name})
                result.update({"type": self.get_data_type_array(data[0])})

            else:
                result.update({"name": field_name})
                result.update({"type": self.get_data_type_array_with_record(field_name, data[0])})

        return result

    def generate_avro(self):
        if os.path.isdir(self.path):
            for filename in os.listdir(self.path):
                if filename.endswith(".json"):
                    file_path = os.path.join(self.path, filename)
                    self.file_name = os.path.splitext(filename)[0]
                    self.read_json(file_path)
                    for key in self.data:
                        self.fields.append(self.generate_dict_for_field(key, self.data))
                    self.write_json(self.file_name)
        elif os.path.isfile(self.path) and self.path.endswith(".json"):
            self.file_name = os.path.splitext(os.path.basename(self.path))[0]
            self.read_json(self.path)
            for key in self.data:
                self.fields.append(self.generate_dict_for_field(key, self.data))
            self.write_json(self.file_name)
        else:
            print("Invalid JSON file or directory path.")

    def get_record_type(self, prefix, data):
        list_record = []
        for d in data:
            record = []
            if isinstance(d.get("type"), list):
                record.append(prefix + d.get("name"))
                record.append(self.convert_key(d.get("name")))

                if isinstance(d.get("type")[1], dict):
                    record.append(d.get("type")[1].get("items"))
                else:
                    record.append(d.get("type")[1])
                list_record.append(record)
            else:
                new_dict = d.get("type")
                if new_dict.get("type") == "record":
                    list_record = list_record + self.get_record_type(prefix + d.get("name") + "-",
                                                                     new_dict.get("fields"))
                else:
                    fields = new_dict.get("items")
                    list_record = list_record + self.get_record_type(prefix + d.get("name") + "-", fields.get("fields"))
        return list_record

    def get_key_and_data_type(self):
        list_record = self.get_record_type("", self.fields)

        header = ['avro_field_name', 'transformed_name', 'data_type']
        with open("result/check.csv", "w", newline="\n") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(list_record)


class TransformHandler:
    def __init__(self, publisher, prefix, name):
        self.path = "result/check.csv"
        self.data = []
        self.fields = []
        self.publisher = publisher
        self.prefix = prefix
        self.name = name
        self.result_path = f"result/transform_{self.prefix}.avsc"

    def read_csv(self):
        with open(self.path, 'r') as file:
            reader = csv.DictReader(file)
            self.data = [row for row in reader]

    def write_json(self):
        final_result = {
            'namespace': f'com.unified.avroschemas.{self.publisher.lower()}.{self.name.lower()}',
            'type': 'record',
            'doc': f'This Schema describes {self.publisher} {self.name}',
            'name': f'Transform{self.name[:1].upper()}{self.name[1:]}',
            'fields': self.fields
        }

        with open(self.result_path, 'w') as file:
            json.dump(final_result, file)

    def generate_fields(self):
        for d in self.data:
            field = {}
            field["name"] = d["transformed_name"]
            field["type"] = ["null"]
            if d["data_type"] == "boolean":
                field["type"].append("boolean")
            elif d["data_type"] == "int":
                field["type"].append("int")
            elif d["data_type"] == "long":
                field["type"].append("long")
            elif d["data_type"] == "double":
                field["type"].append("double")
            elif d["data_type"] == "string":
                field["type"].append("string")

            self.fields.append(field)

    def generate_transformed_avro(self):
        self.read_csv()
        self.generate_fields()
        self.write_json()


# Example usage
path = input("Enter the path to the JSON file(s) or directory containing JSON files: ")
publisher = input("Enter the publisher name: ")

avro_handler = AvroHandler(path, publisher)
avro_handler.generate_avro()
avro_handler.get_key_and_data_type()

transform_handler = TransformHandler(publisher, avro_handler.file_name.lower(), avro_handler.file_name)
transform_handler.generate_transformed_avro()
