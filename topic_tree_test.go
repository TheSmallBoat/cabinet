package cabinet

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestTopicTree(t *testing.T) {
	defer goleak.VerifyNone(t)

	tt := NewTopicTree()

	require.NoError(t, tt.EntityLink([]byte("sports/tennis/+/stats"), "ent1"))
	require.Error(t, tt.EntityLink([]byte("sports/tennis/+/stats"), nil))

	require.Error(t, tt.EntityUnLink([]byte("sports/tennis"), "ent1"))

	entities := make([]interface{}, 0, 5)

	require.NoError(t, tt.ConnectedEntities([]byte("sports/tennis/tom/stats"), &entities))
	require.Equal(t, 1, len(entities))
	require.Equal(t, "ent1", entities[0])

	require.NoError(t, tt.EntityUnLink([]byte("sports/tennis/+/stats"), "ent1"))
	require.Equal(t, 0, len(tt.root.nltNodes))
	require.Equal(t, 0, len(tt.root.entities))
}

func BenchmarkTopicTreeLink(b *testing.B) {
	tt := NewTopicTree()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := []byte(fmt.Sprintf("sports/tennis/+/%d/stats", i))
		entity := fmt.Sprintf("ent_%d", i)
		err := tt.EntityLink(topic, entity)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTopicTreeUnLink(b *testing.B) {
	tt := NewTopicTree()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := []byte(fmt.Sprintf("sports/tennis/joe/%d/stats", i))
		entity := fmt.Sprintf("ent_%d", i)
		require.NoError(b, tt.EntityLink(topic, entity))

		topic = []byte(fmt.Sprintf("sports/tennis/tom/%d/stats", i))
		entity = fmt.Sprintf("ent_%d", i)
		require.NoError(b, tt.EntityLink(topic, entity))

		err := tt.EntityUnLink(topic, entity)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTopicTreeConnectedEntities(b *testing.B) {
	tt := NewTopicTree()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := tt.EntityLink([]byte("sports/tennis/+/stats"), "ent1")
		if err != nil {
			b.Fatal(err)
		}
		err = tt.EntityLink([]byte("sports/tennis/tom/stats"), "ent2")
		if err != nil {
			b.Fatal(err)
		}

		err = tt.EntityLink([]byte("sports/tennis/jack/stats"), "ent3")
		if err != nil {
			b.Fatal(err)
		}

		entities := make([]interface{}, 0, 5)

		err = tt.ConnectedEntities([]byte("sports/tennis/tom/stats"), &entities)
		if err != nil {
			b.Fatal(err)
		}
		require.Equal(b, 2, len(entities))
	}
}
