package bdb

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// NewOutPointFromString returns a new bitcoin transaction outpoint parsed from
// the provided string, which should be in the format "hash:index".
func NewOutPointFromString(outpoint string) (*wire.OutPoint, error) {
	parts := strings.Split(outpoint, ":")
	if len(parts) != 2 {
		return nil, errors.New("outpoint should be of the form txid:index")
	}
	hash, err := chainhash.NewHashFromStr(parts[0])
	if err != nil {
		return nil, err
	}

	outputIndex, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid output index: %v", err)
	}

	return &wire.OutPoint{
		Hash:  *hash,
		Index: uint32(outputIndex),
	}, nil
}
