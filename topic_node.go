package cabinet

import (
	"fmt"
	"reflect"
)

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	SEP = "/"

	// SYS is the starting character of the system level topics
	SYS = "$"

	// Both wildcards
	_WC = "#+"
)

const (
	stateCHR byte = iota // Regular character
	stateMWC             // Multi-level wildcard
	stateSWC             // Single-level wildcard
	stateSEP             // Topic level separator
	stateSYS             // System level topic ($)
)

type tNode struct {
	// If this is the end of the topic string, the add entity here
	entities []interface{}

	// Otherwise add the next topic level here
	nltNodes map[string]*tNode
}

func newTopicNode() *tNode {
	return topicNodePool.acquire()
}

func (tn *tNode) close() error {
	tn.entities = tn.entities[0:0]
	for level, nltn := range tn.nltNodes {
		delete(tn.nltNodes, level)
		err := nltn.close()
		if err != nil {
			return fmt.Errorf("%s, found in next level: '%s'", err, level)
		}
	}
	if len(tn.entities) == 0 && len(tn.nltNodes) == 0 {
		topicNodePool.release(tn)
		return nil
	} else {
		return fmt.Errorf("topicNode/close: Cleanup not completed, still have [%d] entities, [%d] next level nodes", len(tn.entities), len(tn.nltNodes))
	}
}

func (tn *tNode) insertEntity(topic []byte, entity interface{}) error {
	// If there's no more topic levels, that means we are at the matching tNode
	// to insert the body. So let's see if there's such entity,
	// if so, return. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the entity is already on the list. If yes, return
		for i := range tn.entities {
			if equal(tn.entities[i], entity) {
				return nil
			}
		}
		// Otherwise add.
		tn.entities = append(tn.entities, entity)

		return nil
	}

	// Not the last level, so let's find or create the next level tNode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add tNode if it doesn't already exist
	nltn, ok := tn.nltNodes[level]
	if !ok {
		nltn = newTopicNode()
		tn.nltNodes[level] = nltn
	}

	return nltn.insertEntity(rem, entity)
}

// the entity matches then it's removed
func (tn *tNode) removeEntity(topic []byte, entity interface{}) error {
	// If the topic is empty, it means we are at the final matching tNode. If so,
	// let's find the matching entities and remove them.
	if len(topic) == 0 {
		// If entity == nil, then it's signal to remove ALL entities
		if entity == nil {
			tn.entities = tn.entities[0:0]
			return nil
		}

		// If we find the entity then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range tn.entities {
			if equal(tn.entities[i], entity) {
				tn.entities = append(tn.entities[:i], tn.entities[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("topicNode/remove: No topic found for entity")
	}
	// Not the last level, so let's find the next level tNode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the tNode that matches the topic level
	nltn, ok := tn.nltNodes[level]
	if !ok {
		return fmt.Errorf("topicNode/remove: No topic found")
	}

	// Remove the entity from the next level tNode
	if err := nltn.removeEntity(rem, entity); err != nil {
		return err
	}

	// If there are no more entities and nltNodes to the next level we just visited
	// let's remove it
	if len(nltn.entities) == 0 && len(nltn.nltNodes) == 0 {
		delete(tn.nltNodes, level)
		topicNodePool.release(nltn)
	}

	return nil
}

func (tn *tNode) appendEntities(entities *[]interface{}) {
	for _, entity := range tn.entities {
		*entities = append(*entities, entity)
	}
}

// match() returns all the entities that are link to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of entities that link
// to the topic. For each of the level names, it's a match
// - if there are entities to '#', then all the entities are added to result set
func (tn *tNode) matchEntities(topic []byte, entities *[]interface{}) error {
	// If the topic is empty, it means we are at the final matching tNode. If so,
	// let's find the entities, and append them to the list.
	if len(topic) == 0 {
		tn.appendEntities(entities)
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, nltn := range tn.nltNodes {
		// If the key is "#", then these entities are added to the result set
		if k == MWC {
			nltn.appendEntities(entities)
		} else if k == SWC || k == level {
			if err := nltn.matchEntities(rem, entities); err != nil {
				return err
			}
		}
	}

	return nil
}

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, fmt.Errorf("topicNode/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return nil, nil, fmt.Errorf("topicNode/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return nil, nil, fmt.Errorf("topicNode/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = stateSWC

			//case '$':
			//	if i == 0 {
			//		return nil, nil, fmt.Errorf("topicNode/nextTopicLevel: Cannot link to $ topics")
			//	}

			//	s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, fmt.Errorf("topicNode/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

func equal(k1, k2 interface{}) bool {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}

	if reflect.ValueOf(k1).Kind() == reflect.Func {
		return &k1 == &k2
	}

	if k1 == k2 {
		return true
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)

	case int64:
		return k1 == k2.(int64)

	case int32:
		return k1 == k2.(int32)

	case int16:
		return k1 == k2.(int16)

	case int8:
		return k1 == k2.(int8)

	case int:
		return k1 == k2.(int)

	case float32:
		return k1 == k2.(float32)

	case float64:
		return k1 == k2.(float64)

	case uint:
		return k1 == k2.(uint)

	case uint8:
		return k1 == k2.(uint8)

	case uint16:
		return k1 == k2.(uint16)

	case uint32:
		return k1 == k2.(uint32)

	case uint64:
		return k1 == k2.(uint64)

	case uintptr:
		return k1 == k2.(uintptr)
	}

	return false
}
