import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import io


def exemplo_um():
    schema = avro.schema.parse(open("resources/user.avsc", "rb").read())

    writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
    writer.append({"name": "Alyssa", "favorite_number": 256})
    writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
    writer.close()

    reader = DataFileReader(open("users.avro", "rb"), DatumReader())
    for user in reader:
        print(user)
    reader.close()

def exemplo_dois():
    schema = avro.schema.parse(open("resources/user.avsc", "rb").read())
    writer = avro.io.DatumWriter(schema)

    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"name": "Alyssa", "favorite_number": 256}, encoder)
    writer.write({"name": "Ben", "favorite_number": 7, "favorite_color": "red"}, encoder)

    raw_bytes = bytes_writer.getvalue()
    print(len(raw_bytes))
    print(type(raw_bytes))

    bytes_reader = io.BytesIO(raw_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    user1 = reader.read(decoder)
    user2 = reader.read(decoder)

    print(user1)
    print(user2)

def exemplo_tres():
    reader = DataFileReader(open("users.avro", "rb"), DatumReader())
    for user in reader:
        print(user)
    reader.close()

exemplo_tres()