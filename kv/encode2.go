package kv

// TODO 数据块压缩

func EnCodeSSTable2(sst *SSTable2) ([]byte, error) {
	var allBlockLen uint64 = 0
	for _, block := range sst.DataBlocks {
		allBlockLen += 8
		allBlockLen += uint64(len(block.Key))
		allBlockLen += uint64(len(block.Value))
	}

	allBytes := make([]byte, 0, allBlockLen)
	for _, block := range sst.DataBlocks {
		allBytes = append(allBytes, Uint32ToBytes(uint32(len(block.Key)))...)
		allBytes = append(allBytes, block.Key...)
		allBytes = append(allBytes, Uint32ToBytes(uint32(len(block.Value)))...)
		allBytes = append(allBytes, block.Value...)
	}
	return allBytes, nil
}
