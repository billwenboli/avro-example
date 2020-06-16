package com.billwenboli.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;

public class AvroGenericImport {

    public static void avroFileImport(File file) throws IOException {

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(new File("/Users/liwenbobill/Documents/Development/avro-example/src/main/resources/schemas/person.avsc"));

        DatumReader<GenericRecord> datumReader;
        if (avroSchema != null) {
            datumReader = new GenericDatumReader<>(avroSchema);
        } else {
            datumReader = new GenericDatumReader<>();
        }

        GenericRecord avroRecord = null;

        try {
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
            avroRecord = dataFileReader.next();

            if (avroRecord == null) {
                System.out.println("Record was null");
            }

            System.out.println(avroRecord.toString());
        } catch (IOException ioe) {
            throw new IOException("Error deserializing Avro" + ioe);
        }
    }

    public static void avroStreamImport(byte[] input) throws IOException {

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(new File("/Users/liwenbobill/Documents/Development/avro-example/src/main/resources/schemas/person.avsc"));

        DatumReader<GenericRecord> datumReader;
        if (avroSchema != null) {
            datumReader = new GenericDatumReader<>(avroSchema);
        } else {
            datumReader = new GenericDatumReader<>();
        }

        GenericRecord avroRecord = null;

        try {
            SeekableByteArrayInput sbai = new SeekableByteArrayInput(input);
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(sbai, datumReader);
            avroRecord = dataFileReader.next();

            if (avroRecord == null) {
                System.out.println("Record was null");
            }

            System.out.println(avroRecord.toString());
        } catch (IOException ioe) {
            throw new IOException("Error deserializing Avro" + ioe);
        }
    }

    public static void avroDecodeImport(byte[] input) throws IOException {

        Schema.Parser parser = new Schema.Parser();
        Schema avroSchema = parser.parse(new File("/Users/liwenbobill/Documents/Development/avro-example/src/main/resources/schemas/person.avsc"));

        DatumReader<GenericRecord> datumReader;
        if (avroSchema != null) {
            datumReader = new GenericDatumReader<>(avroSchema);
        } else {
            datumReader = new GenericDatumReader<>();
        }

        GenericRecord avroRecord = null;

        try {
            Decoder datumDecoder = DecoderFactory.get().binaryDecoder(input, (BinaryDecoder) null);
            avroRecord = datumReader.read(null, datumDecoder);

            if (avroRecord == null) {
                System.out.println("Record was null");
            }

            System.out.println(avroRecord.toString());
        } catch (IOException ioe) {
            throw new IOException("Error deserializing Avro" + ioe);
        }
    }
}
