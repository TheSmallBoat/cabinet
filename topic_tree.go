package cabinet

import (
	"fmt"
	"sync"
)

type TTree struct {
	mu sync.RWMutex

	root *tNode // topic tree root node
}

func NewTopicTree() *TTree {
	return &TTree{root: newTopicNode()}
}

func (tr *TTree) EntityLink(topic []byte, entity interface{}) error {
	if entity == nil {
		return fmt.Errorf("topicTree/EntityLink: entry cannot be nil")
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.root.insertEntity(topic, entity)
}

func (tr *TTree) EntityUnLink(topic []byte, entity interface{}) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.root.removeEntity(topic, entity)
}

// Returned values will be invalidated by the next ConnectedEntities call
func (tr *TTree) LinkedEntities(topic []byte, entities *[]interface{}) error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	*entities = (*entities)[0:0]

	return tr.root.matchEntities(topic, entities)
}

func (tr *TTree) Close() error {
	err := tr.root.close()
	tr.root = nil

	return err
}
