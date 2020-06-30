package cabinet

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNextTopicLevelSuccess(t *testing.T) {
	defer goleak.VerifyNone(t)

	topics := [][]byte{
		[]byte("sport/tennis/player1/#"),
		[]byte("sport/tennis/player1/ranking"),
		[]byte("sport/#"),
		[]byte("#"),
		[]byte("sport/tennis/#"),
		[]byte("+"),
		[]byte("+/tennis/#"),
		[]byte("sport/+/player1"),
		[]byte("/finance"),
	}

	levels := [][][]byte{
		{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("#")},
		{[]byte("sport"), []byte("tennis"), []byte("player1"), []byte("ranking")},
		{[]byte("sport"), []byte("#")},
		{[]byte("#")},
		{[]byte("sport"), []byte("tennis"), []byte("#")},
		{[]byte("+")},
		{[]byte("+"), []byte("tennis"), []byte("#")},
		{[]byte("sport"), []byte("+"), []byte("player1")},
		{[]byte("+"), []byte("finance")},
	}

	for i, topic := range topics {
		var (
			tl  []byte
			rem = topic
			err error
		)

		for _, level := range levels[i] {
			tl, rem, err = nextTopicLevel(rem)
			require.NoError(t, err)
			require.Equal(t, level, tl)
		}
	}
}

func TestNextTopicLevelFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	topics := [][]byte{
		[]byte("sport/tennis#"),
		[]byte("sport/tennis/#/ranking"),
		[]byte("sport+"),
	}

	var (
		rem []byte
		err error
	)

	_, rem, err = nextTopicLevel(topics[0])
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(topics[1])
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.NoError(t, err)

	_, rem, err = nextTopicLevel(rem)
	require.Error(t, err)

	_, rem, err = nextTopicLevel(topics[2])
	require.Error(t, err)
}

func TestTopicNodeInsert1(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("sport/tennis/player1/#")

	err := n.insertEntity(topic, "ent1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))

	n2, ok := n.nltNodes["sport"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.nltNodes))
	require.Equal(t, 0, len(n2.entities))

	n3, ok := n2.nltNodes["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.nltNodes))
	require.Equal(t, 0, len(n3.entities))

	n4, ok := n3.nltNodes["player1"]
	require.True(t, ok)
	require.Equal(t, 1, len(n4.nltNodes))
	require.Equal(t, 0, len(n4.entities))

	n5, ok := n4.nltNodes["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n5.nltNodes))
	require.Equal(t, 1, len(n5.entities))
	require.Equal(t, "ent1", n5.entities[0].(string))
}

func TestTopicNodeInsert2(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("#")

	err := n.insertEntity(topic, "ent1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))

	n2, ok := n.nltNodes["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n2.nltNodes))
	require.Equal(t, 1, len(n2.entities))
	require.Equal(t, "ent1", n2.entities[0].(string))
}

func TestTopicNodeInsert3(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("+/tennis/#")

	err := n.insertEntity(topic, "ent1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))

	n2, ok := n.nltNodes["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.nltNodes))
	require.Equal(t, 0, len(n2.entities))

	n3, ok := n2.nltNodes["tennis"]
	require.True(t, ok)
	require.Equal(t, 1, len(n3.nltNodes))
	require.Equal(t, 0, len(n3.entities))

	n4, ok := n3.nltNodes["#"]
	require.True(t, ok)
	require.Equal(t, 0, len(n4.nltNodes))
	require.Equal(t, 1, len(n4.entities))
	require.Equal(t, "ent1", n4.entities[0].(string))
}

func TestTopicNodeInsert4(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("/finance")

	err := n.insertEntity(topic, "ent1")

	require.NoError(t, err)
	require.Equal(t, 1, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))

	n2, ok := n.nltNodes["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.nltNodes))
	require.Equal(t, 0, len(n2.entities))

	n3, ok := n2.nltNodes["finance"]
	require.True(t, ok)
	require.Equal(t, 0, len(n3.nltNodes))
	require.Equal(t, 1, len(n3.entities))
	require.Equal(t, "ent1", n3.entities[0].(string))
}

func TestTopicNodeInsertDup(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("/finance")

	err := n.insertEntity(topic, "ent1")
	err = n.insertEntity(topic, "ent1")
	require.NoError(t, err)
	require.Equal(t, 1, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))

	n2, ok := n.nltNodes["+"]
	require.True(t, ok)
	require.Equal(t, 1, len(n2.nltNodes))
	require.Equal(t, 0, len(n2.entities))

	n3, ok := n2.nltNodes["finance"]
	require.True(t, ok)
	require.Equal(t, 0, len(n3.nltNodes))
	require.Equal(t, 1, len(n3.entities))
	require.Equal(t, "ent1", n3.entities[0].(string))
}

func TestTopicNodeRemove1(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.insertEntity(topic, "ent1"))
	err := n.removeEntity([]byte("sport/tennis/player1/#"), "ent1")
	require.NoError(t, err)
	require.Equal(t, 0, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))
}

func TestTopicNodeRemove2(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.insertEntity(topic, "ent1"))
	err := n.removeEntity([]byte("sport/tennis/player1"), "ent1")
	require.Error(t, err)
	require.Equal(t, 1, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))
}

func TestTopicNodeRemove3(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("sport/tennis/player1/#")

	require.NoError(t, n.insertEntity(topic, "ent1"))
	require.NoError(t, n.insertEntity(topic, "ent2"))
	err := n.removeEntity([]byte("sport/tennis/player1/#"), nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(n.nltNodes))
	require.Equal(t, 0, len(n.entities))
}

func TestTopicNodeMatch1(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	topic := []byte("sport/tennis/player1/#")
	require.NoError(t, n.insertEntity(topic, "ent1"))

	entities := make([]interface{}, 0, 5)

	err := n.matchEntities([]byte("sport/tennis/player1/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
}

func TestTopicNodeMatch2(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	require.NoError(t, n.insertEntity([]byte("sport/tennis/+/tom"), "ent1"))
	require.NoError(t, n.insertEntity([]byte("sport/tennis/player1/tom"), "ent2"))

	entities := make([]interface{}, 0, 5)

	err := n.matchEntities([]byte("sport/tennis/player1/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 2, len(entities))
}

func TestTopicNodeMatch3(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	require.NoError(t, n.insertEntity([]byte("sport/tennis/#"), "ent1"))
	require.NoError(t, n.insertEntity([]byte("sport/tennis"), "ent2"))

	entities := make([]interface{}, 0, 5)

	err := n.matchEntities([]byte("sport/tennis/player1/tom"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	require.Equal(t, "ent1", entities[0])
}

func TestTopicNodeMatch4(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	require.NoError(t, n.insertEntity([]byte("+/+"), "ent1"))

	entities := make([]interface{}, 0, 5)

	err := n.matchEntities([]byte("/finance"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	require.Equal(t, "ent1", entities[0])
}

func TestTopicNodeMatch5(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	require.NoError(t, n.insertEntity([]byte("/+"), "ent1"))

	entities := make([]interface{}, 0, 5)

	err := n.matchEntities([]byte("/finance"), &entities)
	require.NoError(t, err)
	require.Equal(t, 1, len(entities))
	require.Equal(t, "ent1", entities[0])
}

func TestTopicNodeMatch9(t *testing.T) {
	defer goleak.VerifyNone(t)

	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(t, err)
	}()

	require.NoError(t, n.insertEntity([]byte("+"), "ent1"))

	entities := make([]interface{}, 0, 5)

	err := n.matchEntities([]byte("/finance"), &entities)
	require.NoError(t, err)
	require.Equal(t, 0, len(entities))
}

func BenchmarkTopicNode(b *testing.B) {
	n := newTopicNode()
	defer func() {
		err := n.close()
		require.NoError(b, err)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < 32; i++ {
		ti := []byte(fmt.Sprintf("sport/%d/#", i))
		require.NoError(b, n.insertEntity(ti, "ent1"))
		for j := 0; j < 32; j++ {
			tj := []byte(fmt.Sprintf("sport/%d/+/%d/#", i, j))
			require.NoError(b, n.insertEntity(tj, "ent2"))
			for k := 0; k < 32; k++ {
				tk := []byte(fmt.Sprintf("sport/%d/player/%d/%d", i, j, k))
				require.NoError(b, n.insertEntity(tk, "ent3"))
				require.NoError(b, n.removeEntity(tk, "ent3"))
			}
			require.NoError(b, n.removeEntity(tj, "ent2"))
		}
		require.NoError(b, n.removeEntity(ti, "ent1"))
	}
}
