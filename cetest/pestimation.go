package cetest

import "sync"

type PEstResultCollector interface {
	AppendEstResults(ers []EstResult)
	EstResults(tblIdx int) []EstResult
}

func NewPEstResultCollector() PEstResultCollector {
	return new(pEstResultCollector)
}

type pEstResultCollector struct {
	rs   [][]EstResult
	lock sync.RWMutex
}

func (p *pEstResultCollector) AppendEstResults(ers []EstResult) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.rs = append(p.rs, ers)
}

func (p *pEstResultCollector) EstResults(tblIdx int) []EstResult {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.rs[tblIdx]
}
