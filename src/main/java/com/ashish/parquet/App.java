package com.ashish.parquet;

import com.ashish.parquet.avro.Trade;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        Path filePath = new Path("md/Trade.parquet");
        writeParquet(filePath);
        Thread.sleep(5000l);
        readParquet(filePath);
    }

    private static void writeParquet(Path filePath) throws IOException {
        Schema avroSchema = Trade.getClassSchema();
        OutputFile outputFile = HadoopOutputFile.fromPath(filePath, new Configuration());
        try (ParquetWriter<Trade> writer = AvroParquetWriter.<Trade>builder(outputFile)
                .withSchema(avroSchema).withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withConf(new Configuration())
                .build()) {
            long start = System.nanoTime();
            for (int i = 0; i <2000000 ; i++) {
                writer.write(getTrade());
            }
            long end = System.nanoTime();
            System.out.println("Total time take to write 2 million records (ns)"+(end-start));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Trade getTrade() {
        return Trade.newBuilder()
                .setTime(System.currentTimeMillis())
                .setExchange("NSE")
                .setPrice(ThreadLocalRandom.current().nextDouble(99.0))
                .setSize(ThreadLocalRandom.current().nextLong(999))
                .setSymbol("ICICI").build();
    }

    private static void readParquet(Path filePath) throws IOException {
        InputFile inputFile = HadoopInputFile.fromPath(filePath, new Configuration());
        try (ParquetReader<Trade> parquetReader = AvroParquetReader.<Trade>builder(inputFile).withConf(new Configuration()).build()) {
            Trade trade;
            while ((trade = parquetReader.read()) != null) {
                System.out.println(trade);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
