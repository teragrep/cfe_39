package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.avro.SyslogRecord;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SyncableFileOutputStream;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;
import java.nio.ByteBuffer;

class SyslogAvroWriter implements AutoCloseable {

    private final DatumWriter<SyslogRecord> datumWriter =
            new SpecificDatumWriter<>(SyslogRecord.class);

    private final SyncableFileOutputStream syncableFileOutputStream;

    private final DataFileWriter<SyslogRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

    SyslogAvroWriter(File syslogFile) throws IOException {
        dataFileWriter.setCodec(CodecFactory.snappyCodec());


        syncableFileOutputStream =
                new SyncableFileOutputStream(syslogFile);

        syncableFileOutputStream.getChannel().tryLock();

        if (syslogFile.length() == 0) {
            // new file
            dataFileWriter.create(SyslogRecord.getClassSchema(), syncableFileOutputStream);
        }
        else {
            // existing file
            SeekableFileInput seekableFileInput = new SeekableFileInput(syslogFile);

            // seek to end
            syncableFileOutputStream.getChannel().position(syncableFileOutputStream.getChannel().size());
            dataFileWriter.appendTo(seekableFileInput, syncableFileOutputStream);
        }
    }


    void write(SyslogRecord syslogRecord) throws IOException{
        dataFileWriter.append(syslogRecord);
        dataFileWriter.flush(); // TODO: Avro files 'flush' must be called as few times as possible. Check memory usage impact
    }

    public void close() throws IOException {
        dataFileWriter.close();
    }

    public long getFileSize() throws IOException {
        return syncableFileOutputStream.getChannel().size();
    }
}