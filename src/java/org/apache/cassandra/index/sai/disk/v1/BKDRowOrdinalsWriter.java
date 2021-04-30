package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.SortedRow;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter.BLOCK_SIZE;

public class BKDRowOrdinalsWriter
{
    // note: BKDReader must be closed by the caller
    public BKDRowOrdinalsWriter(BKDReader reader,
                                IndexComponents components,
                                final SegmentMetadata.ComponentMetadataMap metadata) throws IOException
    {
        final List<BKDReader.RowIDAndPointIDIterator> iterators = reader.rowIDIterators();
        final RowIDPointIDMergeIterator merger = new RowIDPointIDMergeIterator(iterators.toArray(new BKDReader.RowIDAndPointIDIterator[0]));

        final IndexOutput output = components.createOutput(components.kdTreeRowOrdinals, true);

        final long offset = output.getFilePointer();

        //System.out.println("rowIDToPointOrdinal2="+Arrays.toString(rowIDToPointOrdinal2));

        // NumericValuesMeta meta = new NumericValuesMeta(count, blockSize, fp);

        final BlockPackedWriter blockPackedWriter = new BlockPackedWriter(output, BLOCK_SIZE);

//        final NumericValuesWriter writer = new NumericValuesWriter(components.kdTreeRowOrdinals,
//                                                                   output,
//                                                                   metadataWriter,
//                                                                   false);

        //metadata.put(IndexComponents.NDIType.KD_TREE_ROW_ORDINALS, );

        long count = 0;

        while (merger.hasNext())
        {
            final BKDReader.RowIDAndPointID obj = merger.next();
            blockPackedWriter.add(obj.pointID);
            count++;
        }

        final long fp = blockPackedWriter.finish();

        final long length = output.getFilePointer() - offset;

        Map<String, String> additionalMap = new HashMap();
        additionalMap.put("count", count+"");
        additionalMap.put("blockSize", BLOCK_SIZE+"");

        metadata.put(IndexComponents.NDIType.KD_TREE_ROW_ORDINALS, fp, offset, length, additionalMap);

        merger.close();

        //blockPackedWriter.close();
        output.close();
    }

    // note: BKDReader must be closed by the caller
    public BKDRowOrdinalsWriter(BKDReader reader,
                                IndexComponents components,
                                MetadataWriter metadataWriter) throws IOException
    {
        List<BKDReader.RowIDAndPointIDIterator> iterators = reader.rowIDIterators();
        RowIDPointIDMergeIterator merger = new RowIDPointIDMergeIterator(iterators.toArray(new BKDReader.RowIDAndPointIDIterator[0]));

        final IndexOutput output = components.createOutput(components.kdTreeRowOrdinals, true);

        //System.out.println("rowIDToPointOrdinal2="+Arrays.toString(rowIDToPointOrdinal2));

        final NumericValuesWriter writer = new NumericValuesWriter(components.kdTreeRowOrdinals,
                                                                   output,
                                                                   metadataWriter,
                                                                   false);

        while (merger.hasNext())
        {
            final BKDReader.RowIDAndPointID obj = merger.next();
            writer.add(obj.pointID);
        }

        merger.close();

        writer.close();
        output.close();
    }

    public static BlockPackedReader openReader(IndexComponents components,
                                               MetadataSource meta,
                                               SortedRow.SortedRowFactory keyFactory) throws IOException
    {
        final FileHandle file = components.createFileHandle(components.kdTreeRowOrdinals);

        return new BlockPackedReader(file, keyFactory, components, meta);
    }
}
