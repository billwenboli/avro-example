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

        // Specific Set
        File specificFile = AvroSpecificExport.avroFileExport();
        AvroSpecificImport.avroFileImport(specificFile);

        byte[] specificResult = AvroSpecificExport.avroStreamExport();
        AvroSpecificImport.avroStreamImport(specificResult);
    }
}
