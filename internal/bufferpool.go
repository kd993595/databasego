package internal

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

/*type BufferPool interface {
	FetchPage(tablePos int, pageid PageID) *InternalPage
	DeletePage(tableName string, pageid PageID)
	GetPageDisk(pageid PageID) (*InternalPage, error) //disk manager
	InsertData(pageid PageID, data [][]byte) (uint64, error)
	SelectDataRange(start PageID, end PageID) []*InternalPage
	Unpin(pageid PageID)
	Victim() int
}*/

type PageID uint64

type InternalPage struct {
	buf      [PAGESIZE]byte
	pincount atomic.Int32
	id       PageID
	pinned   bool
}

type BufferPoolManager struct {
	dir      string
	allpools map[string]bufferPool
}

type bufferPool struct {
	slots          [MAXPOOLSIZE]*InternalPage
	freelist       []int
	mxread         *sync.Mutex
	mxwrite        *sync.Mutex
	pagemx         *sync.RWMutex
	alltables      map[PageID]int
	tablefileRead  *os.File
	tablefileWrite *os.File
}

func NewBufferPool(dir string) *BufferPoolManager {
	newManager := BufferPoolManager{
		dir:      dir,
		allpools: make(map[string]bufferPool),
	}

	return &newManager
}

func (bm *BufferPoolManager) NewPool(tablename string, dir string) {
	newPool := bufferPool{
		slots:    [MAXPOOLSIZE]*InternalPage{},
		freelist: make([]int, MAXPOOLSIZE),
		mxread:   &sync.Mutex{},
		mxwrite:  &sync.Mutex{},
		pagemx:   &sync.RWMutex{},
	}
	for i := 0; i < MAXPOOLSIZE; i++ {
		newPool.freelist[i] = i
	}
	for i := range newPool.slots {
		newPool.slots[i] = &InternalPage{
			buf:      [PAGESIZE]byte{},
			pincount: atomic.Int32{},
			id:       0,
			pinned:   false,
		}
	}
	filePathStr := filepath.Join(dir, tablename, ".db")
	_, err := os.Stat(filePathStr)
	if err != nil {
		os.Create(filePathStr)
	}
	newPool.tablefileRead, _ = os.OpenFile(filePathStr, os.O_RDONLY, 0644)
	newPool.tablefileWrite, _ = os.OpenFile(filePathStr, os.O_WRONLY, 0644)
	bm.allpools[tablename] = newPool
}

func (bm *BufferPoolManager) ManagerFetchPage(tablename string, pageid PageID) (*InternalPage, error) {
	pool, ok := bm.allpools[tablename]
	if !ok {
		return nil, errors.New(fmt.Sprintf("table name: \"%s\" does not exist", tablename))
	}
	return pool.FetchPage(pageid), nil
}

// returns nil if error occured
func (b *bufferPool) FetchPage(pageid PageID) *InternalPage {
	b.pagemx.RLock()
	pagepos, ok := b.alltables[pageid]
	if ok {
		tmppage := b.slots[pagepos]
		tmppage.pincount.Add(1)
		tmppage.pinned = true
		b.pagemx.RUnlock()
		return tmppage
	}
	//getframeid and have to allocate page from buffer if none free
	b.pagemx.RUnlock()
	frameId := b.GetFrameID(pageid)
	b.slots[frameId].pincount.Store(1)
	b.slots[frameId].id = pageid
	err := b.AllocatePage(pageid, frameId)
	if err != nil {
		return nil
	}
	return b.slots[frameId]
}

func (b *bufferPool) GetFrameID(pageid PageID) int {
	b.pagemx.Lock()
	defer b.pagemx.Unlock()
	if len(b.freelist) > 0 {
		frameID, newFreeList := b.freelist[0], b.freelist[1:]
		b.freelist = newFreeList
		b.slots[frameID].pinned = true
		return frameID
	}
	//return clockreplacer scan through pages pincount
	for {
		frameID := 0
		for i := range b.slots {
			if !b.slots[i].pinned && b.slots[i].pincount.Load() <= 0 {
				delete(b.alltables, pageid)
				frameID = i
				break
			}

		}
		b.slots[frameID].pinned = true
		return frameID
	}
}

// read page from disk
func (b *bufferPool) AllocatePage(pageid PageID, frameId int) error {
	b.mxread.Lock()
	defer b.mxread.Unlock()

	_, err := b.tablefileRead.Seek(int64(pageid)*PAGESIZE, 0)
	if err != nil {
		return err
	}
	buf := make([]byte, PAGESIZE)
	_, err = b.tablefileRead.Read(buf)
	if err != nil {
		return err
	}

	b.slots[frameId].buf = [4096]byte(buf)
	return nil
}

func (b *bufferPool) Unpin(frameId int) {
	b.slots[frameId].pincount.Add(-1)
	if b.slots[frameId].pincount.Load() <= 0 {
		b.slots[frameId].pinned = false
	}
}

func (bm *BufferPoolManager) UnpinPage(tablename string, frameid int) {
	pool, _ := bm.allpools[tablename]
	pool.Unpin(frameid)
}

// return last page modified
func (bm *BufferPoolManager) InsertData(tablename string, pageid PageID, data [][]byte) (uint64, error) {

	pool, ok := bm.allpools[tablename]
	if !ok {
		return 0, errors.New(fmt.Sprintf("table name: \"%s\" does not exist", tablename))
	}
	pageToModify := pool.FetchPage(pageid)
	if pageToModify == nil {
		return 0, errors.New("internal error fetching page")
	}
	buf := pageToModify.buf //pointer
	pool.mxwrite.Lock()
	defer pool.mxwrite.Unlock()
	f := pool.tablefileWrite

	//numberOfPage := binary.LittleEndian.Uint64(buf[0:8])
	rowNums := binary.LittleEndian.Uint16(buf[8:10])
	//checksum := buf[10:26]

	offset := 26 + int(rowNums)*len(data[0])
	rows := 0
	pgNum := pageid
	for i := range data {
		if offset+len(data[i]) > PAGESIZE {
			//create new page and write old page
			binary.LittleEndian.PutUint64(buf[0:8], uint64(pgNum))
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
			offset = 26
		}
		copy(buf[offset:], data[i])
		offset += len(data[0])
		rows += 1
	}

	checksum := md5.Sum(buf[26:])
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

func (b *BufferPoolManager) SelectDataRange(start, end PageID) []*InternalPage {
	return nil
}
