package wsps

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type Example struct {
	Number int
}

func TestDecodeInterfaceType(t *testing.T) {
	encode := NewEvent("test", uuid.New(), &Example{Number: 42})
	encoded, err := json.Marshal(&encode)
	assert.NoError(t, err)

	decode := Event{Content: &Example{}}
	err = json.Unmarshal(encoded, &decode)
	assert.NoError(t, err)

	assert.Equal(t, encode.Topic, decode.Topic)
	assert.Equal(t, encode.Stream, decode.Stream)
	assert.Equal(t, encode.Content, decode.Content)
}
