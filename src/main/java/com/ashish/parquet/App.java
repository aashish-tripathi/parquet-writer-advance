package com.ashish.parquet;

import com.ashish.parquet.avro.Trade;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class App {
    public static void main(String[] args) throws IOException {
        Schema avroSchema = Trade.getClassSchema();
        Trade[] trades = new Trade[]{Trade.newBuilder()
                .setTime(System.currentTimeMillis())
                .setExchange("NSE")
                .setPrice(ThreadLocalRandom.current().nextDouble(99.0))
                .setSize(ThreadLocalRandom.current().nextLong(999))
                .setSymbol("ICICI").build(), Trade.newBuilder()
                .setTime(System.currentTimeMillis())
                .setExchange("NSE")
                .setPrice(ThreadLocalRandom.current().nextDouble(99.0))
                .setSize(ThreadLocalRandom.current().nextLong(999))
                .setSymbol("ICICI").build(), Trade.newBuilder()
                .setTime(System.currentTimeMillis())
                .setExchange("NSE")
                .setPrice(ThreadLocalRandom.current().nextDouble(99.0))
                .setSize(ThreadLocalRandom.current().nextLong(999))
                .setSymbol("ICICI").build(), Trade.newBuilder()
                .setTime(System.currentTimeMillis())
                .setExchange("NSE")
                .setPrice(ThreadLocalRandom.current().nextDouble(99.0))
                .setSize(ThreadLocalRandom.current().nextLong(999))
                .setSymbol("ICICI").build()};

        Path filePath = new Path("md/Trade.parquet");
        writeParquet(avroSchema, trades, filePath);
        readParquet(filePath);
    }

    private static void writeParquet(Schema avroSchema, Trade[] dataToWrite, Path filePath) {
        try (ParquetWriter<Trade> writer = AvroParquetWriter.<Trade>builder(filePath)
                .withSchema(avroSchema).withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withConf(new Configuration())
                .build()) {
            for (Trade obj : dataToWrite) {
                writer.write(obj);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void readParquet(Path filePath) throws IOException {
        InputFile inputFile = HadoopInputFile.fromPath(filePath, new Configuration());
        try (ParquetReader<Trade> parquetReader = AvroParquetReader.<Trade>builder(inputFile).withConf(new Configuration()).build()) {
            Trade trade;
            while ((trade = parquetReader.read()) != null) {
                System.out.println(trade);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
