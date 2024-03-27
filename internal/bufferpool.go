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
	slotid   int
	pinned   bool
}

type BufferPoolManager struct {
	dir      string
	allpools map[string]*bufferPool
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
		allpools: make(map[string]*bufferPool),
	}

	return &newManager
}

func (bm *BufferPoolManager) NewPool(tablename string, dir string) {
	newPool := &bufferPool{
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
			slotid:   -1,
			pinned:   false,
		}
	}
	filePathStr := filepath.Join(dir, fmt.Sprintf("%s.db", tablename))
	_, err := os.Stat(filePathStr)
	if err != nil {
		os.Create(filePathStr)
	}
	newPool.tablefileRead, _ = os.OpenFile(filePathStr, os.O_RDONLY, 0644)
	newPool.tablefileWrite, _ = os.OpenFile(filePathStr, os.O_WRONLY, 0644)
	bm.allpools[tablename] = newPool
}

// func (bm *BufferPoolManager) ManagerFetchPage(tablename string, pageid PageID) (*InternalPage, int, error) {
// 	pool, ok := bm.allpools[tablename]
// 	if !ok {
// 		return nil, -1, fmt.Errorf("table name: \"%s\" does not exist", tablename)
// 	}
// 	page, frameid := pool.FetchPage(pageid)
// 	return page, frameid, nil
// }

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
		b.slots[frameID].slotid = frameID
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
		b.slots[frameID].slotid = frameID
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

func (b *bufferPool) DeletePage(pageid PageID, frameid int) {
	b.pagemx.Lock()
	defer b.pagemx.Unlock()

	delete(b.alltables, pageid)
	b.freelist = append(b.freelist, frameid)
}

func (bm *BufferPoolManager) UnpinPage(tablename string, frameid int) {
	pool := bm.allpools[tablename]
	pool.Unpin(frameid)
}

// return last page modified
func (bm *BufferPoolManager) InsertData(tablename string, pageid PageID, data [][]byte) (PageID, error) {

	pool, ok := bm.allpools[tablename]
	if !ok {
		return 0, fmt.Errorf("table name: \"%s\" does not exist", tablename)
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

	pgNum := pageid
	for i := range data {
		if offset+len(data[i]) > PAGESIZE {
			//create new page and write old page
			binary.LittleEndian.PutUint64(buf[0:8], uint64(pgNum))
			binary.LittleEndian.PutUint16(buf[8:10], rowNums)
			checksum := md5.Sum(buf[26:])
			copy(buf[10:26], checksum[:])

			f.Seek(int64(pgNum)*PAGESIZE, 0)
			_, err := f.Write(buf[:])
			if err != nil {
				return 0, err
			}
			rowNums = 0
			pgNum += 1
			buf = [PAGESIZE]byte{}
			binary.LittleEndian.PutUint64(buf[:], uint64(pgNum))
			offset = 26
		}
		copy(buf[offset:], data[i])
		offset += len(data[0])
		rowNums += 1
		fmt.Println("another row")
	}

	checksum := md5.Sum(buf[26:])
	copy(buf[10:26], checksum[:])
	binary.LittleEndian.PutUint16(buf[8:10], rowNums)

	f.Seek(int64(pgNum)*PAGESIZE, 0)
	_, err := f.Write(buf[:])
	if err != nil {
		return 0, err
	}

	f.Sync()

	//deletes page from bufferpool and all data should be written to disk by this point
	pool.DeletePage(pageid, pageToModify.slotid)

	return pgNum, nil
}

func (bm *BufferPoolManager) SelectDataRange(tablename string, start, end PageID) []*InternalPage {
	allpages := make([]*InternalPage, 0, end-start)

	pool := bm.allpools[tablename]
	for i := start; i <= end; i++ {
		page := pool.FetchPage(i)
		allpages = append(allpages, page)
	}
	return allpages
}
