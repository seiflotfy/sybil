package pkg

import "sort"

type TrimSpec struct {
	MBLimit      int64 // size limit of DB in megabytes
	DeleteBefore int64 // delete records older than DeleteBefore in seconds
}

// List all the blocks that should be trimmed to keep the table within it's
// memory limits
func (t *Table) TrimTable(trimSpec *TrimSpec) []*TableBlock {
	t.LoadRecords(nil)
	Debug("TRIMMING TABLE, MEMORY Limit", trimSpec.MBLimit, "TIME Limit", trimSpec.DeleteBefore)

	blocks := make([]*TableBlock, 0)
	to_trim := make([]*TableBlock, 0)

	for _, b := range t.BlockList {
		if b.Name == ROW_STORE_BLOCK {
			continue
		}

		block := t.LoadBlockFromDir(b.Name, nil, false)
		if block != nil {
			if block.Info.IntInfoMap[*Flags.TimeCol] != nil {
				block.table = t
				blocks = append(blocks, block)
			}
		}
	}

	// Sort the blocks by descending Max Time
	sort.Sort(sort.Reverse(SortBlocksByEndTime(blocks)))

	size := int64(0)
	bytes_in_megabytes := int64(1024 * 1024)
	for _, b := range blocks {

		info := b.Info.IntInfoMap[*Flags.TimeCol]
		trim := false
		if trimSpec.MBLimit > 0 && size/bytes_in_megabytes >= trimSpec.MBLimit {
			trim = true
		}

		if info.Max < trimSpec.DeleteBefore {
			trim = true
		}

		if trim {
			to_trim = append(to_trim, b)
		}

		size += b.Size
	}

	return to_trim
}
