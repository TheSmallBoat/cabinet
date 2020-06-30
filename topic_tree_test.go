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
	defer func() {
		err := tt.Close()
		require.NoError(t, err)
	}()

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

func BenchmarkTopicTree(b *testing.B) {
	entities := make([]interface{}, 0)

	tt := NewTopicTree()
	defer func() {
		err := tt.Close()
		require.NoError(b, err)
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < 32; i++ {
		ti := []byte(fmt.Sprintf("sport/%d/#", i))
		require.NoError(b, tt.EntityLink(ti, "ent1"))
		require.NoError(b, tt.LinkedEntities(ti, &entities))
		for j := 0; j < 32; j++ {
			tj := []byte(fmt.Sprintf("sport/%d/+/%d/#", i, j))
			require.NoError(b, tt.EntityLink(tj, "ent2"))
			require.NoError(b, tt.LinkedEntities(tj, &entities))
			for k := 0; k < 32; k++ {
				tk := []byte(fmt.Sprintf("sport/%d/player/%d/%d", i, j, k))
				require.NoError(b, tt.EntityLink(tk, "ent3"))
				require.NoError(b, tt.LinkedEntities(tk, &entities))

				require.NoError(b, tt.EntityUnLink(tk, "ent3"))
				require.NoError(b, tt.LinkedEntities(tk, &entities))
			}
			for k := 0; k < 32; k++ {
				tk := []byte(fmt.Sprintf("sport/%d/tom/%d/%d", i, j, k))
				require.NoError(b, tt.EntityLink(tk, "ent4"))
				require.NoError(b, tt.LinkedEntities(tk, &entities))

				require.NoError(b, tt.EntityUnLink(tk, "ent4"))
				require.NoError(b, tt.LinkedEntities(tk, &entities))
			}
			for k := 0; k < 32; k++ {
				tk := []byte(fmt.Sprintf("sport/%d/jack/%d/%d", i, j, k))
				require.NoError(b, tt.EntityLink(tk, "ent5"))
				require.NoError(b, tt.LinkedEntities(tk, &entities))

				require.NoError(b, tt.EntityUnLink(tk, "ent5"))
				require.NoError(b, tt.LinkedEntities(tk, &entities))
			}
			require.NoError(b, tt.EntityUnLink(tj, "ent2"))
		}
		require.NoError(b, tt.EntityUnLink(ti, "ent1"))
	}
}
