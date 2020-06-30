package cabinet

import (
	"sync"
)

var topicNodePool = tNodePool{sp: sync.Pool{}}

type tNodePool struct {
	sp sync.Pool
}

func (p *tNodePool) acquire() *tNode {
	v := p.sp.Get()
	if v == nil {
		v = &tNode{nltNodes: make(map[string]*tNode)}
	}
	tn := v.(*tNode)
	return tn
}

func (p *tNodePool) release(tn *tNode) {
	p.sp.Put(tn)
}
