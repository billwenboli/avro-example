package com.billwenboli.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class AvroSpecificExport {

    public static File avroFileExport() throws IOException {

        DatumWriter<BdPerson> datumWriter = new SpecificDatumWriter<BdPerson>(BdPerson.class);

        BdPerson bdPerson = BdPerson.newBuilder()
                .setId(1)
                .setUsername("mrscarter")
                .setFirstName("Beyonce")
                .setMiddleName(null)
                .setLastName("Knowles-Carter")
                .setBirthdate("1981-09-04")
                .setJoinDate("2016-01-01")
                .setEmailAddress("testemail@example.com")
                .setPhoneNumber("1111111")
                .setSex("F")
                .setPreviousLogins(null)
                .setLastIp(null)
                .build();

        try {
            File file = new File("person.avro");
            DataFileWriter<BdPerson> dataFileWriter = new DataFileWriter<BdPerson>(datumWriter);

            dataFileWriter.create(bdPerson.getSchema(), file);
            dataFileWriter.append(bdPerson);
            dataFileWriter.close();

            return file;
        } catch (IOException ioe) {
            throw new IOException("Error serializing Avro" + ioe);
        }
    }

    public static byte[] avroStreamExport() throws IOException {

        DatumWriter<BdPerson> datumWriter = new SpecificDatumWriter<BdPerson>(BdPerson.class);

        BdPerson bdPerson = BdPerson.newBuilder()
                .setId(1)
                .setUsername("mrscarter")
                .setFirstName("Beyonce")
                .setMiddleName(null)
                .setLastName("Knowles-Carter")
                .setBirthdate("1981-09-04")
                .setJoinDate("2016-01-01")
                .setEmailAddress("testemail@example.com")
                .setPhoneNumber("1111111")
                .setSex("F")
                .setPreviousLogins(null)
                .setLastIp(null)
                .build();

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataFileWriter<BdPerson> dataFileWriter = new DataFileWriter<BdPerson>(datumWriter);

            dataFileWriter.create(bdPerson.getSchema(), baos);
            dataFileWriter.append(bdPerson);
            dataFileWriter.flush();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IOException("Error serializing Avro" + ioe);
        }
    }

    public static byte[] avroEncodeExport() throws IOException {

        DatumWriter<BdPerson> datumWriter = new SpecificDatumWriter<BdPerson>(BdPerson.class);

        BdPerson bdPerson = BdPerson.newBuilder()
                .setId(1)
                .setUsername("mrscarter")
                .setFirstName("Beyonce")
                .setMiddleName(null)
                .setLastName("Knowles-Carter")
                .setBirthdate("1981-09-04")
                .setJoinDate("2016-01-01")
                .setEmailAddress("testemail@example.com")
                .setPhoneNumber("1111111")
                .setSex("F")
                .setPreviousLogins(null)
                .setLastIp(null)
                .build();

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Encoder datumEncoder = EncoderFactory.get().binaryEncoder(baos, (BinaryEncoder) null);

            datumWriter.write(bdPerson, datumEncoder);
            datumEncoder.flush();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new IOException("Error serializing Avro" + ioe);
        }
    }
}
