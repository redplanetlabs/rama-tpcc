package rpl.tpcc;

import java.io.IOException;

import org.rocksdb.*;

import com.rpl.rama.RocksDBOptionsBuilder;

public class TPCCRocksDBOptionsBuilder implements RocksDBOptionsBuilder {
  BloomFilter bloomFilter;
  LRUCache blockCache;
  BlockBasedTableConfig tableConfig;

  public static final int BLOCK_CACHE_MB = 24 * 1024;

  public TPCCRocksDBOptionsBuilder() {
    bloomFilter = new BloomFilter(10, false);
    blockCache = new LRUCache(BLOCK_CACHE_MB * 1024 * 1024);
    tableConfig = new BlockBasedTableConfig();
    tableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
    tableConfig.setFilterPolicy(bloomFilter);
    tableConfig.setPartitionFilters(true);
    tableConfig.setMetadataBlockSize(4096);
    tableConfig.setCacheIndexAndFilterBlocks(true);
    tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    tableConfig.setPinTopLevelIndexAndFilter(true);
    tableConfig.setPinL0FilterAndIndexBlocksInCache(true);
  }

  @Override
  public void setOptions(String pstateName, Options options) {
    tableConfig.setBlockCache(blockCache);
    options.setTableFormatConfig(tableConfig);
    options.setMaxOpenFiles(3000);
    options.setMaxLogFileSize(1024 * 500);
    options.setKeepLogFileNum(2);
    options.setAtomicFlush(false);
    options.setCompressionType(CompressionType.LZ4_COMPRESSION);
  }

  @Override
  public void close() throws IOException {
    blockCache.close();
    bloomFilter.close();
  }
}
