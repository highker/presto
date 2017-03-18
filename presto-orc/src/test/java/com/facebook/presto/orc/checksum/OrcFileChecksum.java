/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.checksum;

import com.facebook.presto.orc.AbstractOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;

@SuppressWarnings("UseOfSystemOutOrSystemErr")
public final class OrcFileChecksum
{
    private final long rowCount;
    private final List<Long> checksums;

    public static void main(String... args)
            throws Exception
    {
        Path path = new Path(args[0]);

        checksumHdfsOrcFile(path, false);
    }

    private static void checksumHdfsOrcFile(Path path, boolean isDwrf)
            throws IOException
    {
        FileSystem fileSystem = path.getFileSystem(new Configuration());

        FileStatus fileStatus = fileSystem.getFileStatus(path);
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        OrcDataSource orcDataSource = new HdfsOrcDataSource(
                path.toString(),
                fileStatus.getLen(),
                new DataSize(1, MEGABYTE),
                new DataSize(8, MEGABYTE),
                new DataSize(8, MEGABYTE),
                fsDataInputStream);

        OrcFileChecksum orcFileChecksum = new OrcFileChecksum(orcDataSource, isDwrf);
        System.out.println();
        System.out.println("rows: " + orcFileChecksum.getRowCount());
        List<Long> checksums = orcFileChecksum.getChecksums();
        for (int i = 0; i < checksums.size(); i++) {
            System.out.println(i + ": " + Long.toUnsignedString(checksums.get(i), 16));
        }
    }

    public OrcFileChecksum(OrcDataSource orcDataSource, boolean isDwrf)
            throws IOException
    {
        MetadataReader metadataReader;
        if (isDwrf) {
            metadataReader = new DwrfMetadataReader();
        }
        else {
            metadataReader = new OrcMetadataReader();
        }
        OrcReader orcReader = new OrcReader(orcDataSource, metadataReader, new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE));

        List<Type> types = toPrestoTypes(orcReader.getFooter().getTypes());
        
        ImmutableMap.Builder<Integer, Type> includedColumnsBuilder = ImmutableMap.builder();
        for (int i = 0; i < types.size(); i++) {
            includedColumnsBuilder.put(i, types.get(i));
        }
        Map<Integer, Type> includedColumns = includedColumnsBuilder.build();

        OrcRecordReader recordReader = orcReader.createRecordReader(includedColumns, OrcPredicate.TRUE, DateTimeZone.getDefault(), new AggregatedMemoryContext());

        long rowCount = 0;
        long[] checksums = new long[types.size()];
        for (int batchSize = recordReader.nextBatch(); batchSize > 0; batchSize = recordReader.nextBatch()) {
            readBatch(recordReader, types, checksums, batchSize);
            rowCount += batchSize;
        }
        this.rowCount = rowCount;
        this.checksums = ImmutableList.copyOf(Longs.asList(checksums));
    }

    private static List<Type> toPrestoTypes(List<OrcType> orcTypes)
    {
        OrcType tableType = orcTypes.get(0);
        checkArgument(tableType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int i = 0; i < tableType.getFieldCount(); i++) {
            types.add(toPrestoType(orcTypes.get(tableType.getFieldTypeIndex(i)), orcTypes));
        }
        return types.build();
    }

    private static Type toPrestoType(OrcType orcType, List<OrcType> orcTypes)
    {
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return TINYINT;
            case SHORT:
                return SMALLINT;
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case DECIMAL:
                return createDecimalType(orcType.getPrecision().get(), orcType.getScale().get());
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
            case VARCHAR:
            case CHAR:
                return VARCHAR;
            case BINARY:
                return VARBINARY;
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case LIST:
                return new ArrayType(toPrestoType(orcTypes.get(orcType.getFieldTypeIndex(0)), orcTypes));
            case MAP:
                return new MapType(
                        toPrestoType(orcTypes.get(orcType.getFieldTypeIndex(0)), orcTypes),
                        toPrestoType(orcTypes.get(orcType.getFieldTypeIndex(1)), orcTypes));
            case STRUCT:
                return new RowType(
                        IntStream.range(0, orcType.getFieldCount())
                                .map(orcType::getFieldTypeIndex)
                                .mapToObj(orcTypes::get)
                                .map(fieldType -> toPrestoType(fieldType, orcTypes))
                                .collect(toList()),
                        Optional.of(IntStream.range(0, orcType.getFieldCount())
                                .mapToObj(orcType::getFieldName)
                                .collect(toList())));
            default:
                throw new IllegalArgumentException("Unsupported type kind: " + orcType.getOrcTypeKind());
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<Long> getChecksums()
    {
        return checksums;
    }

    private static void readBatch(OrcRecordReader recordReader, List<Type> columnTypes, long[] checksums, int batchSize)
            throws IOException
    {
        for (int columnIndex = 0; columnIndex < checksums.length; columnIndex++) {
            Type type = columnTypes.get(columnIndex);
            Block block = recordReader.readBlock(type, columnIndex);
            verify(batchSize == block.getPositionCount());
            for (int position = 0; position < block.getPositionCount(); position++) {
                long valueHash = hashBlockPosition(type, block, position);
                checksums[columnIndex] = combineHash(checksums[columnIndex], valueHash);
            }
        }
    }

    private static long combineHash(long currentHash, long valueHash)
    {
        return 31 * currentHash + valueHash;
    }

    private static long hashBlockPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return 0;
        }
        return type.hash(block, position);
    }

    private static List<Type> toHiveTypes(String hiveTypes, TypeManager typeManager)
    {
        requireNonNull(hiveTypes, "hiveTypes is null");
        List<TypeInfo> hiveTypeInfos = getTypeInfosFromTypeString(hiveTypes);

        return ImmutableList.copyOf(hiveTypeInfos.stream()
                .map(OrcFileChecksum::getTypeSignature)
                .map(typeManager::getType)
                .collect(toList()));
    }

    private static TypeSignature getTypeSignature(TypeInfo typeInfo)
    {
        switch (typeInfo.getCategory()) {
            case PRIMITIVE:
                Type primitiveType = getPrimitiveType((PrimitiveTypeInfo) typeInfo);
                if (primitiveType == null) {
                    break;
                }
                return primitiveType.getTypeSignature();
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                TypeSignature keyType = getTypeSignature(mapTypeInfo.getMapKeyTypeInfo());
                TypeSignature valueType = getTypeSignature(mapTypeInfo.getMapValueTypeInfo());
                return new TypeSignature(
                        StandardTypes.MAP,
                        ImmutableList.of(TypeSignatureParameter.of(keyType), TypeSignatureParameter.of(valueType)));
            case LIST:
                ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
                TypeSignature elementType = getTypeSignature(listTypeInfo.getListElementTypeInfo());
                return new TypeSignature(
                        StandardTypes.ARRAY,
                        ImmutableList.of(TypeSignatureParameter.of(elementType)));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<TypeSignature> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos()
                        .stream()
                        .map(OrcFileChecksum::getTypeSignature)
                        .collect(toList());
                return new TypeSignature(StandardTypes.ROW, fieldTypes, structTypeInfo.getAllStructFieldNames());
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type: %s", typeInfo));
    }

    private static Type getPrimitiveType(PrimitiveTypeInfo typeInfo)
    {
        switch (typeInfo.getPrimitiveCategory()) {
            case BOOLEAN:
                return BOOLEAN;
            case BYTE:
                return TINYINT;
            case SHORT:
                return SMALLINT;
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return createUnboundedVarcharType();
            case VARCHAR:
                return createVarcharType(((VarcharTypeInfo) typeInfo).getLength());
            case CHAR:
                return createCharType(((CharTypeInfo) typeInfo).getLength());
            case DATE:
                return DATE;
            case TIMESTAMP:
                return TIMESTAMP;
            case BINARY:
                return VARBINARY;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                return createDecimalType(decimalTypeInfo.precision(), decimalTypeInfo.scale());
            default:
                return null;
        }
    }

    private static class HdfsOrcDataSource
            extends AbstractOrcDataSource
    {
        private final FSDataInputStream inputStream;

        public HdfsOrcDataSource(String name, long size, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize, FSDataInputStream inputStream)
        {
            super(name, size, maxMergeDistance, maxReadSize, streamBufferSize);
            this.inputStream = inputStream;
        }

        @Override
        public void close()
                throws IOException
        {
            inputStream.close();
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            try {
                inputStream.readFully(position, buffer, bufferOffset, bufferLength);
            }
            catch (PrestoException e) {
                // just in case there is a Presto wrapper or hook
                throw e;
            }
            catch (Exception e) {
                String message = format("HDFS error reading from %s at position %s", this, position);
                if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, message, e);
                }
                throw new PrestoException(GENERIC_INTERNAL_ERROR, message, e);
            }
        }
    }
}
