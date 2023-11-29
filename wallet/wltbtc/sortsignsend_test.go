package wltbtc

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/dev-warrior777/go-electrum-client/wallet"
)

func createTxStore() (*TxStore, error) {
	mockDb := MockDatastore{
		&mockConfig{creationDate: time.Now()},
		&mockStorage{blob: make([]byte, 10)},
		&mockKeyStore{make(map[string]*keyStoreEntry)},
		&mockUtxoStore{make(map[string]*wallet.Utxo)},
		&mockStxoStore{make(map[string]*wallet.Stxo)},
		&mockTxnStore{make(map[string]*wallet.Txn)},
		&mockSubscribeScriptsStore{make(map[string][]byte)},
	}
	seed := make([]byte, 32)
	rand.Read(seed)
	key, _ := hdkeychain.NewMaster(seed, &chaincfg.RegressionNetParams)
	km, _ := NewKeyManager(mockDb.Keys(), &chaincfg.RegressionNetParams, key)
	return NewTxStore(&chaincfg.TestNet3Params, &mockDb, km)
}

func MockWallet() *BtcElectrumWallet {
	txstore, _ := createTxStore()

	return &BtcElectrumWallet{
		txstore:     txstore,
		keyManager:  txstore.keyManager,
		params:      &chaincfg.RegressionNetParams,
		feeProvider: wallet.DefaultFeeProvider(),
	}
}

func Test_gatherCoins(t *testing.T) {
	w := MockWallet()
	w.blockchainTip = 100
	txid := "6f7a58ad92702601fcbaac0e039943a384f5274a205c16bb8bbab54f9ea2fbad"
	h1, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		t.Error(err)
	}
	key1, err := w.keyManager.GetFreshKey(wallet.EXTERNAL)
	if err != nil {
		t.Error(err)
	}
	addr1, err := key1.Address(&chaincfg.RegressionNetParams)
	if err != nil {
		t.Error(err)
	}
	script1, err := w.AddressToScript(addr1)
	if err != nil {
		t.Error(err)
	}
	op := wire.OutPoint{
		Hash:  *h1,
		Index: 0,
	}
	err = w.txstore.Utxos().Put(wallet.Utxo{Op: op, ScriptPubkey: script1, AtHeight: 5, Value: 10000})
	if err != nil {
		t.Error(err)
	}
	coinmap := w.gatherCoins()
	for coin, key := range coinmap {
		if !bytes.Equal(coin.PkScript(), script1) {
			t.Error("Pubkey script in coin is incorrect")
		}
		if coin.Index() != 0 {
			t.Error("Returned incorrect index")
		}
		if !coin.Hash().IsEqual(h1) {
			t.Error("Returned incorrect hash")
		}
		if coin.NumConfs() != int64(w.blockchainTip-5) {
			t.Error("Returned incorrect number of confirmations")
		}
		if coin.Value() != 10000 {
			t.Error("Returned incorrect coin value")
		}
		addr2, err := key.Address(&chaincfg.RegressionNetParams)
		if err != nil {
			t.Error(err)
		}
		if addr2.EncodeAddress() != addr1.EncodeAddress() {
			t.Error("Returned incorrect key")
		}
		key.Zero()
	}
	os.Remove("headers.bin")
}

func Test_newTransaction(t *testing.T) {
	w := MockWallet()
	w.blockchainTip = 100
	// make one utxo
	txid := "6f7a58ad92702601fcbaac0e039943a384f5274a205c16bb8bbab54f9ea2fbad"
	h1, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		t.Error(err)
	}
	key1, err := w.keyManager.GetFreshKey(wallet.EXTERNAL)
	if err != nil {
		t.Error(err)
	}
	addr1, err := key1.Address(&chaincfg.RegressionNetParams)
	if err != nil {
		t.Error(err)
	}
	script1, err := w.AddressToScript(addr1)
	if err != nil {
		t.Error(err)
	}
	op := wire.OutPoint{
		Hash:  *h1,
		Index: 0,
	}
	err = w.txstore.Utxos().Put(wallet.Utxo{Op: op, ScriptPubkey: script1, AtHeight: 5, Value: 200000})
	if err != nil {
		t.Error(err)
	}

	/////////////////////// maybe set up more utxos later ////////////////////

	address, err := btcutil.DecodeAddress("bcrt1q322tg0y2hzyp9zztr7d2twdclhqg88anvzxwwr", &chaincfg.RegressionNetParams)
	if err != nil {
		t.Error(err)
	}
	tx, err := w.Spend(
		int64(100000),
		address,
		wallet.NORMAL,
		"",
		false,
	)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(tx.TxHash().String())
}
