// This code is available on the terms of the project LICENSE.md file,
// also available online at https://blueoakcouncil.org/license/1.0.0.

package electrumx

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

type positional []any

type request struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"` // [] for positional args or {} for named args, no bare types
}

// RPCError represents a JSON-RPC error object.
type RPCError struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Not all servers return correct RPCError struct json. At least one tested:
// 'blockstream.info' returns just a string "verbose transactions are currently
// unsupported" for some requests on both mainnet and testnet.
func (e *RPCError) UnmarshalJSON(b []byte) error {
	type maybeRPCErr struct {
		Code    int
		Message string
	}
	var good maybeRPCErr
	err := json.Unmarshal(b, &good)
	if err == nil {
		e.Code = good.Code
		e.Message = good.Message
		return nil
	}
	var maybeStr string
	err = json.Unmarshal(b, &maybeStr)
	if err == nil {
		e.Code = 0
		e.Message = maybeStr
		return nil
	}
	return errors.New("cannot parse RPCError returned from server")
}

// Error satisfies the error interface.
func (e RPCError) Error() string {
	return fmt.Sprintf("code %d: %q", e.Code, e.Message)
}

type response struct {
	// The "jsonrpc" fields is ignored.
	ID     uint64          `json:"id"`     // response to request
	Method string          `json:"method"` // notification for subscription
	Result json.RawMessage `json:"result"`
	Error  *RPCError       `json:"error"`
}

type ntfn = request // weird but true
type ntfnData struct {
	Params json.RawMessage `json:"params"`
}

func prepareRequest(id uint64, method string, args any) ([]byte, error) {
	// nil args should marshal as [] instead of null.
	if args == nil {
		args = []json.RawMessage{}
	}
	switch rt := reflect.TypeOf(args); rt.Kind() {
	case reflect.Struct, reflect.Slice:
	case reflect.Ptr: // allow pointer to struct
		if rt.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("invalid arg type %v, must be slice or struct", rt)
		}
	default:
		return nil, fmt.Errorf("invalid arg type %v, must be slice or struct", rt)
	}

	params, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal arguments: %v", err)
	}
	req := &request{
		Jsonrpc: "2.0", // electrum wallet seems to respond with 2.0 regardless
		ID:      id,
		Method:  method,
		Params:  params,
	}
	return json.Marshal(req)
}
