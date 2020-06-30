package cabinet

import (
	"fmt"
	"sync"
)

type topicTree struct {
	mu sync.RWMutex

	//topic tree
	root *tNode
}

func NewTopicTree() *topicTree {
	return &topicTree{root: newTopicNode()}
}

func (tr *topicTree) EntityLink(topic []byte, entity interface{}) error {
	if entity == nil {
		return fmt.Errorf("topicTree/EntityLink: entry cannot be nil")
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.root.insertEntity(topic, entity)
}

func (tr *topicTree) EntityUnLink(topic []byte, entity interface{}) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.root.removeEntity(topic, entity)
}

// Returned values will be invalidated by the next ConnectedEntities call
func (tr *topicTree) ConnectedEntities(topic []byte, entities *[]interface{}) error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	*entities = (*entities)[0:0]

	return tr.root.matchEntities(topic, entities)
}

func (tr *topicTree) Close() error {
	tr.root = nil
	return nil
}
