package cabinet

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestTopicGroupNotShare(t *testing.T) {
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
	for _, topic := range topics {
		gn, tp, share, err := getGroupNameFromTopic(topic)
		require.Equal(t, []byte(""), gn)
		require.Equal(t, topic, tp)
		require.Equal(t, false, share)
		require.NoError(t, err)
	}
}

func TestTopicGroupShareSuccess(t *testing.T) {
	defer goleak.VerifyNone(t)

	topics := [][]byte{
		[]byte("$share/sport/tennis/player1/#"),
		[]byte("$share/sport/tennis/player1/ranking"),
		[]byte("$share/sport/#"),
		[]byte("$share/sport/tennis/#"),
		[]byte("$share/sport/+/player1"),
	}
	for _, topic := range topics {
		gn, _, share, err := getGroupNameFromTopic(topic)
		require.Equal(t, []byte("sport"), gn)
		require.Equal(t, true, share)
		require.NoError(t, err)
	}

	gn, tp, share, err := getGroupNameFromTopic([]byte("$share/-/tennis/#"))
	require.Equal(t, []byte("-"), gn)
	require.Equal(t, []byte("tennis/#"), tp)
	require.Equal(t, true, share)
	require.NoError(t, err)
}

func TestTopicGroupShareFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	topics := [][]byte{
		[]byte("$share/#"),
		[]byte("$share/+"),
		[]byte("$share/-"),
		[]byte("$share/+/tennis/#"),
		[]byte("$share/finance"),
	}
	for _, topic := range topics {
		gn, tp, share, err := getGroupNameFromTopic(topic)
		require.Equal(t, []byte(""), gn)
		require.Equal(t, []byte(""), tp)
		require.Equal(t, false, share)
		require.Error(t, err)
	}
}
