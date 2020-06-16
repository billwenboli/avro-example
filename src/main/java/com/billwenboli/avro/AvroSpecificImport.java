package com.billwenboli.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class AvroSpecificImport {

    public static void avroFileImport(File file) throws IOException {

        DatumReader<BdPerson> datumReader = new SpecificDatumReader<BdPerson>(BdPerson.class);

        BdPerson bdPerson = null;

        try {
            DataFileReader<BdPerson> dataFileReader = new DataFileReader<BdPerson>(file, datumReader);
            bdPerson = dataFileReader.next();

            if (bdPerson == null) {
                System.out.println("Record was null");
            }

            System.out.println(bdPerson.toString());
        } catch (IOException ioe) {
            throw new IOException("Error deserializing Avro" + ioe);
        }
    }

    public static void avroStreamImport(byte[] input) throws IOException {

        DatumReader<BdPerson> datumReader = new SpecificDatumReader<BdPerson>(BdPerson.class);

        BdPerson bdPerson = null;

        try {
            SeekableByteArrayInput sbai = new SeekableByteArrayInput(input);
            DataFileReader<BdPerson> dataFileReader = new DataFileReader<BdPerson>(sbai, datumReader);
            bdPerson = dataFileReader.next();

            if (bdPerson == null) {
                System.out.println("Record was null");
            }

            System.out.println(bdPerson.toString());
        } catch (IOException ioe) {
            throw new IOException("Error deserializing Avro" + ioe);
        }
    }

    public static void avroDecodeImport(byte[] input) throws IOException {

        DatumReader<BdPerson> datumReader = new SpecificDatumReader<BdPerson>(BdPerson.class);

        BdPerson bdPerson = null;

        try {
            Decoder datumDecoder = DecoderFactory.get().binaryDecoder(input, (BinaryDecoder) null);
            bdPerson = datumReader.read(null, datumDecoder);

            if (bdPerson == null) {
                System.out.println("Record was null");
            }

            System.out.println(bdPerson.toString());
        } catch (IOException ioe) {
            throw new IOException("Error deserializing Avro" + ioe);
        }
    }
}
