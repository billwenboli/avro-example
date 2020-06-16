package com.billwenboli.avro;

import java.io.File;
import java.io.IOException;

public class DataMain {

    public static void main(String[] args) throws IOException {

        // Generic Set
        File genericFile = AvroGenericExport.avroFileExport();
        AvroGenericImport.avroFileImport(genericFile);

        byte[] genericResult = AvroGenericExport.avroStreamExport();
        AvroGenericImport.avroStreamImport(genericResult);

        byte[] genericEncode = AvroGenericExport.avroEncodeExport();
        AvroGenericImport.avroDecodeImport(genericEncode);

        // Specific Set
        File specificFile = AvroSpecificExport.avroFileExport();
        AvroSpecificImport.avroFileImport(specificFile);

        byte[] specificResult = AvroSpecificExport.avroStreamExport();
        AvroSpecificImport.avroStreamImport(specificResult);

        byte[] specificEncode = AvroSpecificExport.avroEncodeExport();
        AvroSpecificImport.avroDecodeImport(specificEncode);
    }
}
