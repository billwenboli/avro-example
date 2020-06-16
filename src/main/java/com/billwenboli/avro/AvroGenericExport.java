package com.billwenboli.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class AvroGenericExport {

    public static File avroFileExport() throws IOException {

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(new File("/Users/liwenbobill/Documents/Development/avro-example/src/main/resources/schemas/person.avsc"));

        DatumWriter<GenericRecord> datumWriter;
        if (avroSchema != null) {
            datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
        } else {
            datumWriter = new GenericDatumWriter<GenericRecord>();
        }

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", 1);
        avroRecord.put("username", "mrscarter");
        avroRecord.put("first_name", "Beyonce");
        avroRecord.put("last_name", "Knowles-Carter");
        avroRecord.put("birthdate", "1981-09-04");
        avroRecord.put("join_date", "2016-01-01");

        try {
            File file = new File("person.avro");
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

            if (avroSchema != null) {
                dataFileWriter.create(avroSchema, file);
            } else {
                dataFileWriter.create(avroRecord.getSchema(), file);
            }

            dataFileWriter.append(avroRecord);
            dataFileWriter.close();

            return file;
        } catch (IOException ioe) {
            throw new IOException("Error serializing Avro" + ioe);
        }
    }

    public static byte[] avroStreamExport() throws IOException {

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(new File("/Users/liwenbobill/Documents/Development/avro-example/src/main/resources/schemas/person.avsc"));

        DatumWriter<GenericRecord> datumWriter;
        if (avroSchema != null) {
            datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
        } else {
            datumWriter = new GenericDatumWriter<GenericRecord>();
        }

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", 1);
        avroRecord.put("username", "mrscarter");
        avroRecord.put("first_name", "Beyonce");
        avroRecord.put("last_name", "Knowles-Carter");
        avroRecord.put("birthdate", "1981-09-04");
        avroRecord.put("join_date", "2016-01-01");

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

            if (avroSchema != null) {
                dataFileWriter.create(avroSchema, baos);
            } else {
                dataFileWriter.create(avroRecord.getSchema(), baos);
            }

            dataFileWriter.append(avroRecord);
            dataFileWriter.flush();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IOException("Error serializing Avro" + ioe);
        }
    }

    public static byte[] avroEncodeExport() throws IOException {

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(new File("/Users/liwenbobill/Documents/Development/avro-example/src/main/resources/schemas/person.avsc"));

        DatumWriter<GenericRecord> datumWriter;
        if (avroSchema != null) {
            datumWriter = new GenericDatumWriter<GenericRecord>(avroSchema);
        } else {
            datumWriter = new GenericDatumWriter<GenericRecord>();
        }

        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("id", 1);
        avroRecord.put("username", "mrscarter");
        avroRecord.put("first_name", "Beyonce");
        avroRecord.put("last_name", "Knowles-Carter");
        avroRecord.put("birthdate", "1981-09-04");
        avroRecord.put("join_date", "2016-01-01");

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Encoder datumEncoder = EncoderFactory.get().binaryEncoder(baos, (BinaryEncoder) null);

            datumWriter.write(avroRecord, datumEncoder);
            datumEncoder.flush();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IOException("Error serializing Avro" + ioe);
        }
    }
}
