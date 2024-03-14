package btc

import "github.com/dev-warrior777/go-electrum-client/electrumx"

// Return the transaction history of any address. Note: This is a
// walletless server query, results are not checked by SPV.
func (ec *BtcElectrumClient) GetAddressHistory(addr string) (electrumx.HistoryResult, error) {
	node := ec.GetNode()
	if node == nil {
		return nil, ErrNoNode
	}
	scripthash, err := addrToElectrumScripthash(addr, ec.GetConfig().Params)
	if err != nil {
		return nil, err
	}
	return node.GetHistory(scripthash)
}