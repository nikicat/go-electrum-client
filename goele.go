package main

// Run goele as an app

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/dev-warrior777/go-electrum-client/client"
	"github.com/dev-warrior777/go-electrum-client/client/btc"
	"github.com/dev-warrior777/go-electrum-client/electrumx"
	"github.com/dev-warrior777/go-electrum-client/wallet"
)

var (
	coins = []string{"btc"} // add as implemented
	nets  = []string{"mainnet", "testnet", "regtest", "simnet"}
)

var mnemonic = "jungle pair grass super coral bubble tomato sheriff pulp cancel luggage wagon"

func makeBasicConfig(coin, net string) (*client.ClientConfig, error) {
	contains := func(s []string, str string) bool {
		for _, v := range s {
			if v == str {
				return true
			}
		}
		return false
	}
	if !contains(coins, coin) {
		return nil, errors.New("invalid coin")
	}
	if !contains(nets, net) {
		return nil, errors.New("invalid net")
	}
	switch coin {
	case "btc":
	default:
		return nil, errors.New("invalid coin")
	}
	cfg := client.NewDefaultConfig()
	cfg.Chain = wallet.Bitcoin
	cfg.StoreEncSeed = true
	appDir, err := client.GetConfigPath()
	if err != nil {
		return nil, err
	}
	coinNetDir := filepath.Join(appDir, coin, net)
	err = os.MkdirAll(coinNetDir, os.ModeDir|0777)
	if err != nil {
		return nil, err
	}
	cfg.DataDir = coinNetDir
	switch net {
	case "regtest", "simnet":
		cfg.Params = &chaincfg.RegressionNetParams
		cfg.TrustedPeer = electrumx.ServerAddr{
			Net: "ssl", Addr: "127.0.0.1:53002",
		}
		cfg.StoreEncSeed = true
		cfg.Testing = true
	case "testnet":
		cfg.Params = &chaincfg.TestNet3Params
		cfg.TrustedPeer = electrumx.ServerAddr{
			Net: "tcp", Addr: "testnet.aranguren.org:51001",
		}
		cfg.StoreEncSeed = true
		cfg.Testing = true
	case "mainnet":
		cfg.Params = &chaincfg.MainNetParams
		cfg.TrustedPeer = electrumx.ServerAddr{
			Net: "ssl", Addr: "elx.bitske.com:50002",
		}
		cfg.StoreEncSeed = false
		cfg.Testing = false
	}
	return cfg, nil
}

func configure() (*client.ClientConfig, error) {
	coin := flag.String("coin", "btc", "coin name")
	net := flag.String("net", "regtest", "network type; testnet, mainnet, regtest")
	flag.Parse()
	fmt.Println("coin:", *coin)
	fmt.Println("net:", *net)
	return makeBasicConfig(*coin, *net)
}

func main() {
	cfg, err := configure()
	if err != nil {
		fmt.Println(err, " - exiting")
		os.Exit(1)
	}
	fmt.Println(cfg.Chain, cfg.Params.Name)

	ec := btc.NewBtcElectrumClient(cfg)
	ec.CreateNode(client.SingleNode)
	err = ec.GetNode().Start()
	if err != nil {
		fmt.Println(err, " - exiting")
		os.Exit(1)
	}

	// go ec.SyncHeaders()
	// If you do this^ then make an error channel back to here and wait on it. If
	// SyncHeaders fails the client initialization is a fail.

	fmt.Println("syncing headers")
	err = ec.SyncClientHeaders()
	if err != nil {
		ec.GetNode().Stop()
		fmt.Println(err, " - exiting")
		os.Exit(1)
	}

	// start goroutine to listen for blockchain headers arriving
	fmt.Println("subscribe headers")
	err = ec.SubscribeClientHeaders()
	if err != nil {
		ec.GetNode().Stop()
		fmt.Println(err, " - exiting")
		os.Exit(1)
	}

	err = ec.LoadWallet("abc")
	if err != nil {
		ec.GetNode().Stop()
		fmt.Println(err, " - exiting")
		os.Exit(1)
	}

	// Here we would grab current addresses (gap range) for this wallet.
	// Electrum seems to have just 20 external & 10 change..
	// Currently our wallet has 100+100. Consider say 20+20

	// Setup Notify for all those addresses

	// dbg: one address only
	err = ec.SubscribeAddressNotify("mvP2UeXooRghYvsX7H7XVj78FY49jJw6Sq")
	if err != nil {
		ec.GetNode().Stop()
		fmt.Println(err, " - exiting")
		os.Exit(1)
	}

	// start goroutine to listen for scripthash status change notifications arriving
	//TODO:
	sc := ec.GetNode().GetServerConn().SvrConn
	<-sc.Done()
}
