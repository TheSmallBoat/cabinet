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

	require.NoError(t, tt.LinkedEntities([]byte("sports/tennis/tom/stats"), &entities))
	require.Equal(t, 1, len(entities))
	require.Equal(t, "ent1", entities[0])

	require.NoError(t, tt.EntityUnLink([]byte("sports/tennis/+/stats"), "ent1"))
	require.Equal(t, 0, len(tt.root.nltNodes))
	require.Equal(t, 0, len(tt.root.entities))
}

func BenchmarkTopicTreeLink(b *testing.B) {
	tt := NewTopicTree()
	defer func() {
		require.NoError(b, tt.Close())
	}()

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
	defer func() {
		require.NoError(b, tt.Close())
	}()

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

func BenchmarkTopicTreeLinkedEntities(b *testing.B) {
	tt := NewTopicTree()
	defer func() {
		require.NoError(b, tt.Close())
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		topic := []byte(fmt.Sprintf("sports/tennis/joe/%d/stats", i))
		entity := fmt.Sprintf("ent_%d", i)
		require.NoError(b, tt.EntityLink(topic, entity))

		topic = []byte(fmt.Sprintf("sports/tennis/tom/%d/stats", i))
		require.NoError(b, tt.EntityLink(topic, entity))

		topic = []byte(fmt.Sprintf("sports/tennis/+/%d/stats", i))
		require.NoError(b, tt.EntityLink(topic, entity))

		entities := make([]interface{}, 0, 5)
		topic = []byte(fmt.Sprintf("sports/tennis/tom/%d/stats", i))
		require.NoError(b, tt.LinkedEntities(topic, &entities))
		require.Equal(b, 2, len(entities))
	}
}

func BenchmarkParallelTopicTreeLink(b *testing.B) {
	tt := NewTopicTree()
	defer func() {
		require.NoError(b, tt.Close())
	}()

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			topic := []byte(fmt.Sprintf("sports/tennis/joe/%d/stats", i))
			entity := fmt.Sprintf("ent_%d", i)
			require.NoError(b, tt.EntityLink(topic, entity))
			i++
		}
	})
}
