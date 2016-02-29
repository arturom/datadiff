package histogram

type Bin struct {
	Key   int
	Count int
}

type Bins []Bin

func (b Bin) IsFull(h Histogram) bool {
	return b.Count == h.BinCapacity
}

type Histogram struct {
	Bins        Bins
	BinCapacity int
}

func (h1 Histogram) Merge(h2 Histogram) MergedHistogram {
	m := make(PairedBinsMap)
	m.InsertMasterBinCounts(h1.Bins)
	m.UpsertSlaveBinCounts(h2.Bins)

	return MergedHistogram{
		BinPairs:    m,
		BinCapacity: h1.BinCapacity,
	}
}

type PairedBin struct {
	Key             int
	CountFromMaster int
	CountFromSlave  int
}

func (p PairedBin) IsFull(capacity int) bool {
	return p.CountFromMaster == capacity && p.CountFromSlave == capacity
}

type PairedBinsMap map[int]*PairedBin

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

type MergedHistogram struct {
	BinPairs    PairedBinsMap
	BinCapacity int
}

func (h MergedHistogram) UnresolvedPairs() []PairedBin {
	s := make([]PairedBin, 0)
	for _, b := range h.BinPairs {
		if !b.IsFull(h.BinCapacity) {
			s = append(s, *b)
		}
	}
	return s
}
