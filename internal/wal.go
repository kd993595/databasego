package internal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type WalManager struct {
	mu        sync.Mutex
	syncTimer *time.Ticker
	bufWriter *bufio.Writer
	file      *os.File
}

func NewWalManager(dir string, secs int) (*WalManager, error) {
	f, err := os.Open(filepath.Join(dir, "wal.db"))
	if os.IsNotExist(err) {
		f, err = os.Create(filepath.Join(dir, "wal.db"))
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	defer f.Close()

	newManager := WalManager{}
	newManager.file = f
	newManager.mu = sync.Mutex{}
	newManager.syncTimer = time.NewTicker(time.Second * time.Duration(secs))
	newManager.bufWriter = bufio.NewWriter(f)

	return &newManager, nil
}

func (wal *WalManager) InsertRows(pageid PageID, data [][]byte) {
	return

}

// TODO: expose error to database handler
func (wal *WalManager) keepSyncing() {
	for {
		<-wal.syncTimer.C

		wal.mu.Lock()
		err := wal.Sync()
		wal.mu.Unlock()

		if err != nil {
			fmt.Println("error in sync file wal manager")
		}
	}
}

func (wal *WalManager) Sync() error {
	err := wal.bufWriter.Flush()
	if err != nil {
		return err
	}
	return wal.file.Sync()
}
