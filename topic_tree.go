package cabinet

import (
	"fmt"
	"sync"
)

type tTree struct {
	mu sync.RWMutex

	root *tNode // topic tree root node
}

func NewTopicTree() *tTree {
	return &tTree{root: newTopicNode()}
}

func (tr *tTree) EntityLink(topic []byte, entity interface{}) error {
	if entity == nil {
		return fmt.Errorf("topicTree/EntityLink: entry cannot be nil")
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.root.insertEntity(topic, entity)
}

func (tr *tTree) EntityUnLink(topic []byte, entity interface{}) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return tr.root.removeEntity(topic, entity)
}

// Returned values will be invalidated by the next ConnectedEntities call
func (tr *tTree) LinkedEntities(topic []byte, entities *[]interface{}) error {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	*entities = (*entities)[0:0]

	return tr.root.matchEntities(topic, entities)
}

func (tr *tTree) Close() error {
	err := tr.root.close()
	tr.root = nil

	if err != nil {
		return err
	}
	return nil
}
