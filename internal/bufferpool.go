package internal

import (
	"crypto/md5"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type PageID struct {
	tableName string
	pageNum   uint64
}

func (p *PageID) Equals(other PageID) bool {
	return p.pageNum == other.pageNum && p.tableName == other.tableName
}

type InternalPage struct {
	buf      [PAGESIZE]byte
	pincount atomic.Int32
	id       PageID
}

type BufferPoolManager struct {
	slots    [MAXPOOLSIZE]*InternalPage
	freelist []int
	mx       sync.Mutex
	pagemx   sync.RWMutex
	dir      string
}

func NewBufferPool(dir string) *BufferPoolManager {
	newPool := BufferPoolManager{
		slots:    [MAXPOOLSIZE]*InternalPage{},
		freelist: make([]int, MAXPOOLSIZE),
		mx:       sync.Mutex{},
		pagemx:   sync.RWMutex{},
		dir:      dir,
	}
	for i := 0; i < MAXPOOLSIZE; i++ {
		newPool.freelist[i] = i
	}

	return &newPool

}

func (b *BufferPoolManager) FetchPage(pageid PageID) *InternalPage {
	b.pagemx.RLock()
	defer b.pagemx.RUnlock()

	for i, v := range b.slots {
		if v != nil {
			if pageid.Equals(v.id) {
				b.slots[i].pincount.Add(1)
				return b.slots[i]
			}
		}
	}

	return nil
}

func (b *BufferPoolManager) GetPageDisk(pageid PageID) (*InternalPage, error) {
	b.pagemx.Lock()
	defer b.pagemx.Unlock()

	slotID := 0
	if len(b.freelist) > 0 {
		slotID, b.freelist = b.freelist[0], b.freelist[1:]
	} else {
		slotID = b.Victim()
	}
	//read page from disk
	filename := filepath.Join(b.dir, pageid.tableName, ".db")
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	_, err = f.Seek(int64(pageid.pageNum)*PAGESIZE, 0)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, PAGESIZE)
	_, err = f.Read(buf)
	if err != nil {
		return nil, err
	}
	pageCache := &InternalPage{}
	pageCache.buf = [4096]byte(buf)
	pageCache.id = pageid
	pageCache.pincount.Store(1)
	b.slots[slotID] = pageCache
	return pageCache, nil
}

// only call in writer locked thread
func (b *BufferPoolManager) Victim() int {
	for {
		for i, v := range b.slots {
			if v == nil {
				return i
			} else {
				if v.pincount.Load() <= 0 {
					return i
				} else {
					v.pincount.Add(-1)
				}
			}
		}
	}
}

func (b *BufferPoolManager) Unpin(pageid PageID) {
	b.pagemx.RLock()
	defer b.pagemx.RUnlock()

	for i, v := range b.slots {
		if v != nil {
			if pageid.Equals(v.id) {
				b.slots[i].pincount.Add(-1)
				return
			}
		}
	}
}

func (b *BufferPoolManager) DeletePage(pageid PageID) {
	b.pagemx.Lock()
	for i, v := range b.slots {
		if v != nil {
			if pageid.Equals(v.id) {
				b.slots[i] = nil
				b.freelist = append(b.freelist, i)
			}
		}
	}
	b.pagemx.Unlock()
}

// return last page modified
func (b *BufferPoolManager) InsertData(pageid PageID, data [][]byte) (uint64, error) {
	b.mx.Lock()
	defer b.mx.Unlock()

	pageToModify := b.FetchPage(pageid)
	buf := pageToModify.buf

	f, err := os.OpenFile(filepath.Join(b.dir, pageid.tableName, ".db"), os.O_WRONLY, 0666)
	if err != nil {
		return 0, err
	}

	//numberOfPage := binary.LittleEndian.Uint64(buf[0:8])
	rowNums := binary.LittleEndian.Uint16(buf[8:10])
	//checksum := buf[10:26]

	offset := 26 + int(rowNums)*len(data[0])
	rows := 0
	pgNum := pageid.pageNum
	for i := range data {
		if offset+len(data[0]) > PAGESIZE {
			//create new page and write old page
			binary.LittleEndian.PutUint64(buf[0:8], pgNum)
			binary.LittleEndian.PutUint16(buf[8:10], uint16(rows)+rowNums)
			checksum := md5.Sum(buf[26:])
			copy(buf[10:26], checksum[:])

			f.Seek(int64(pgNum)*PAGESIZE, 0)
			_, err = f.Write(buf[:])
			if err != nil {
				return 0, err
			}
			rows = 0
			pgNum += 1
			buf = [PAGESIZE]byte{}
			offset = 18
		}
		copy(buf[offset:], data[i])
		offset += len(data[0])
		rows += 1
	}

	checksum := md5.Sum(buf[18:])
	copy(buf[2:18], checksum[:])
	binary.LittleEndian.PutUint16(buf[0:2], uint16(rows))

	f.Seek(int64(pageid.pageNum)*PAGESIZE, 0)
	_, err = f.Write(buf[:])
	if err != nil {
		return 0, err
	}
	err = f.Sync()
	if err != nil {
		return 0, err
	}

	//remove page from bufferpool so next calls read page again (fix later)
	b.DeletePage(pageid)

	return pgNum, err
}
