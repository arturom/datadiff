package histogram

// Bin represents a histogram bin
type Bin struct {
	Key   int
	Count int
}

// Bin is a slice of bins
type Bins []Bin

// IsFull returns true if the bin is filled to capacity
func (b Bin) IsFull(h Histogram) bool {
	return b.Count == h.BinCapacity
}

// Histogram is a structure composed of bins
type Histogram struct {
	Bins        Bins
	BinCapacity int
}

// Merge combines a second histogram and returns a MergedHistogram
func (h1 Histogram) Merge(h2 Histogram) MergedHistogram {
	m := make(PairedBinsMap)
	m.InsertMasterBinCounts(h1.Bins)
	m.UpsertSlaveBinCounts(h2.Bins)

	return MergedHistogram{
		BinPairs:    m,
		BinCapacity: h1.BinCapacity,
	}
}

// PairedBin describes the counts of records from two different data sources for the same range
type PairedBin struct {
	Key             int
	CountFromMaster int
	CountFromSlave  int
}

// IsFull returns true if the bin is filled to capacity
func (p PairedBin) IsFull(capacity int) bool {
	return p.CountFromMaster == capacity && p.CountFromSlave == capacity
}

// DiffCount returns the difference between the slave source and the master source
func (p PairedBin) DiffCount() int {
	return p.CountFromMaster - p.CountFromSlave
}

// PairedBinsMap describes a map of paired bins where the keys are the bin keys
type PairedBinsMap map[int]*PairedBin

// InsertMasterBinCounts adds all counts from the bins in the master source
func (m PairedBinsMap) InsertMasterBinCounts(b Bins) []int {
	keys := make([]int, len(b))
	for i, bin := range b {
		keys[i] = bin.Key
		m[bin.Key] = &PairedBin{
			Key:             bin.Key,
			CountFromMaster: bin.Count,
			CountFromSlave:  0,
		}
	}
	return keys
}

// UpsertSlaveBinCounts merges the slave counts with the master counts
func (m PairedBinsMap) UpsertSlaveBinCounts(b Bins) []int {
	appended := 0
	keys := make([]int, len(b))

	for _, bin := range b {
		if p, ok := m[bin.Key]; ok {
			p.CountFromSlave = bin.Count
		} else {
			keys[appended] = bin.Key
			appended += appended
			m[bin.Key] = &PairedBin{
				Key:             bin.Key,
				CountFromMaster: 0,
				CountFromSlave:  bin.Count,
			}
		}
	}
	return keys
}

// MergedHistogram is a structure composed of PairedBinsMap and the bin capacity
type MergedHistogram struct {
	BinPairs    PairedBinsMap
	BinCapacity int
}

// UnresolvedPairs returns the pairs that are not filled to capacity
func (h MergedHistogram) UnresolvedPairs() []PairedBin {
	var s []PairedBin
	for _, b := range h.BinPairs {
		if !b.IsFull(h.BinCapacity) {
			s = append(s, *b)
		}
	}
	return s
}
