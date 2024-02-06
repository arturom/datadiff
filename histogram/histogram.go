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
	m.InsertPrimaryBinCounts(h1.Bins)
	m.InsertSecondartyBinCounts(h2.Bins)

	return MergedHistogram{
		BinPairs:    m,
		BinCapacity: h1.BinCapacity,
	}
}

// PairedBin describes the counts of records from two different data sources for the same range
type PairedBin struct {
	Key                int
	CountFromPrimary   int
	CountFromSecondary int
}

// IsFull returns true if the bin is filled to capacity
func (p PairedBin) IsFull(capacity int) bool {
	return p.CountFromPrimary == capacity && p.CountFromSecondary == capacity
}

// DiffCount returns the difference between the slave source and the master source
func (p PairedBin) DiffCount() int {
	return p.CountFromPrimary - p.CountFromSecondary
}

// PairedBinsMap describes a map of paired bins where the keys are the bin keys
type PairedBinsMap map[int]*PairedBin

// InsertPrimaryBinCounts adds all counts from the bins in the master source
func (m PairedBinsMap) InsertPrimaryBinCounts(b Bins) []int {
	keys := make([]int, len(b))
	for i, bin := range b {
		keys[i] = bin.Key
		m[bin.Key] = &PairedBin{
			Key:                bin.Key,
			CountFromPrimary:   bin.Count,
			CountFromSecondary: 0,
		}
	}
	return keys
}

// InsertSecondartyBinCounts merges the slave counts with the master counts
func (m PairedBinsMap) InsertSecondartyBinCounts(b Bins) []int {
	appended := 0
	keys := make([]int, len(b))

	for _, bin := range b {
		if p, ok := m[bin.Key]; ok {
			p.CountFromSecondary = bin.Count
		} else {
			keys[appended] = bin.Key
			appended += appended
			m[bin.Key] = &PairedBin{
				Key:                bin.Key,
				CountFromPrimary:   0,
				CountFromSecondary: bin.Count,
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
