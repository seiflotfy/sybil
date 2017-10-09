package pkg

import "path"
import "os"
import "syscall"
import "fmt"
import "strconv"
import "io/ioutil"
import "time"

var (
	lockUs        = time.Millisecond * 3
	lockTries     = 50
	maxLockBreaks = 5
)

// Every LockFile should have a recovery plan
type RecoverableLock interface {
	Grab() bool
	Release() bool
	Recover() bool
}

var BREAK_MAP = make(map[string]int, 0)

type Lock struct {
	Name   string
	Table  *Table
	broken bool
}

type InfoLock struct {
	Lock
}

type BlockLock struct {
	Lock
}

type CacheLock struct {
	Lock
}

type DigestLock struct {
	Lock
}

func RecoverLock(lock RecoverableLock) bool {
	// TODO: log the auto recovery into a recovery file
	return lock.Recover()
}

func (l *InfoLock) Recover() bool {
	t := l.Lock.Table
	Debug("INFO LOCK RECOVERY")
	dirname := path.Join(*Flags.Dir, t.Name)
	backup := path.Join(dirname, "info.bak")
	infodb := path.Join(dirname, "info.db")

	if t.LoadTableInfoFrom(infodb) {
		Debug("LOADED REASONABLE TABLE INFO, DELETING LOCK")
		l.ForceDeleteFile()
		return true
	}

	if t.LoadTableInfoFrom(backup) {
		Debug("LOADED TABLE INFO FROM BACKUP, RESTORING BACKUP")
		os.Remove(infodb)
		renameAndMod(backup, infodb)
		l.ForceDeleteFile()
		return l.Grab()
	}

	Debug("CANT READ info.db OR RECOVER info.bak")
	Debug("TRY DELETING LOCK BY HAND FOR", l.Name)

	return false
}

func (l *DigestLock) Recover() bool {
	Debug("RECOVERING DIGEST LOCK", l.Name)
	t := l.Table
	ingestdir := path.Join(*Flags.Dir, t.Name, IngestDir)

	os.MkdirAll(ingestdir, 0777)
	// TODO: understand if any file in particular is messing things up...
	pid := int64(os.Getpid())
	l.ForceMakeFile(pid)
	t.RestoreUningestedFiles()
	l.ForceDeleteFile()

	return true
}

func (l *BlockLock) Recover() bool {
	Debug("RECOVERING BLOCK LOCK", l.Name)
	t := l.Table
	tb := t.LoadBlockFromDir(l.Name, nil, true)
	if tb == nil || tb.Info == nil || tb.Info.NumRecords <= 0 {
		Debug("BLOCK IS NO GOOD, TURNING IT INTO A BROKEN BLOCK")
		// This block is not good! need to put it into remediation...
		renameAndMod(l.Name, fmt.Sprint(l.Name, ".broke"))
		l.ForceDeleteFile()
	} else {
		Debug("BLOCK IS FINE, TURNING IT BACK INTO A REAL BLOCK")
		os.RemoveAll(fmt.Sprint(l.Name, ".partial"))
		l.ForceDeleteFile()
	}

	return true
}

func (l *CacheLock) Recover() bool {
	Debug("RECOVERING BLOCK LOCK", l.Name)
	t := l.Table
	files, err := ioutil.ReadDir(path.Join(*Flags.Dir, t.Name, cacheDir))

	if err != nil {
		l.ForceDeleteFile()
		return true
	}

	for _, block_file := range files {
		filename := path.Join(*Flags.Dir, t.Name, cacheDir, block_file.Name())
		block_cache := SavedBlockCache{}

		err := decodeInto(filename, &block_cache)
		if err != nil {
			os.RemoveAll(filename)
			continue
		}

		if err != nil {
			os.RemoveAll(filename)
			Debug("DELETING BAD CACHE FILE", filename)

		}

	}

	l.ForceDeleteFile()

	return true

}

func (l *Lock) Recover() bool {
	Debug("UNIMPLEMENTED RECOVERY FOR LOCK", l.Table.Name, l.Name)
	return false
}

func (l *Lock) ForceDeleteFile() {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*Flags.Dir, t.Name, fmt.Sprintf("%s.lock", digest))

	Debug("FORCE DELETING", lockfile)
	os.RemoveAll(lockfile)
}

func (l *Lock) ForceMakeFile(pid int64) {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*Flags.Dir, t.Name, fmt.Sprintf("%s.lock", digest))

	Debug("FORCE MAKING", lockfile)
	nf, err := os.Create(lockfile)
	if err != nil {
		nf, err = os.OpenFile(lockfile, os.O_CREATE, 0666)
	}

	defer nf.Close()

	nf.WriteString(strconv.FormatInt(pid, 10))
	nf.Sync()

}

func is_active_pid(val []byte) bool {
	// Check if its our PID or not...
	pid_str := strconv.FormatInt(int64(os.Getpid()), 10)
	if pid_str == string(val) {
		return true
	}

	return false
}

func check_if_broken(lockfile string, l *Lock) bool {
	var val []byte
	var err error
	// To check if a PID is active, we... first parse the PID in the file, then
	// we ask the os for the process and send it Signal 0. If the process is
	// alive, there will be no error, or if it isn't owned by us, we'll get an
	// EPERM error
	val, err = ioutil.ReadFile(lockfile)

	var pid_int = int64(0)
	if err == nil {
		pid_int, err = strconv.ParseInt(string(val), 10, 32)

		if err != nil {
			breaks, ok := BREAK_MAP[lockfile]
			if ok {
				breaks = breaks + 1
			} else {
				breaks = 1
			}

			BREAK_MAP[lockfile] = breaks

			Debug("CANT READ PID FROM LOCK:", lockfile, string(val), err, breaks)
			if breaks > maxLockBreaks {
				l.broken = true
				Debug("PUTTING LOCK INTO RECOVERY", lockfile)
			}
			return false
		}
	}

	if err == nil && pid_int != 0 {
		process, err := os.FindProcess(int(pid_int))
		// err is Always nil on *nix
		if err == nil {
			err := process.Signal(syscall.Signal(0))
			if err == nil || err == syscall.EPERM {
				// PROCESS IS STILL RUNNING
			} else {
				time.Sleep(time.Millisecond * 100)
				nextval, err := ioutil.ReadFile(lockfile)

				if err == nil {
					if string(nextval) == string(val) {
						if l.broken {
							Debug("SECOND TRY TO RECOVER A BROKEN LOCK... GIVING UP")
							l.broken = false
							return true
						}

						Debug("OWNER PROCESS IS DEAD, MARKING LOCK FOR RECOVERY", l.Name, val)
						l.broken = true
					}
				}
			}
		}
	}

	return false
}

func check_pid(lockfile string, l *Lock) bool {
	cangrab := false

	var val []byte
	var err error

	// check if the PID is active or not. If the PID isn't active, we enter
	// recovery mode for this Lock() and say it's grabbable
	if check_if_broken(lockfile, l) {
		return true
	}

	for i := 0; i < lockTries; i++ {
		val, err = ioutil.ReadFile(lockfile)

		if err == nil {
			// Check if its our PID or not...
			if is_active_pid(val) {
				return true
			}

			time.Sleep(lockUs)
			continue
		} else {
			cangrab = true
			break
		}
	}

	return cangrab
}

func (l *Lock) Grab() bool {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*Flags.Dir, t.Name, fmt.Sprintf("%s.lock", digest))

	var err error
	for i := 0; i < lockTries; i++ {
		time.Sleep(lockUs)
		if check_pid(lockfile, l) == false {
			if l.broken {
				Debug("MARKING BROKEN LOCKFILE", lockfile)
				return false
			}

			continue
		}

		nf, er := os.Create(lockfile)
		if er != nil {
			err = er
			continue
		}

		defer nf.Close()

		pid := int64(os.Getpid())
		nf.WriteString(strconv.FormatInt(pid, 10))
		Debug("WRITING PID", pid, "TO LOCK", lockfile)
		nf.Sync()

		if check_pid(lockfile, l) == false {
			continue
		}

		Debug("LOCKING", lockfile)
		return true
	}

	Debug("CANT CREATE LOCK FILE:", err)
	Debug("LOCK FAIL!", lockfile)
	return false

}

func (l *Lock) Release() bool {
	t := l.Table
	digest := l.Name

	digest = path.Base(digest)
	// Check to see if this file is locked...
	lockfile := path.Join(*Flags.Dir, t.Name, fmt.Sprintf("%s.lock", digest))
	for i := 0; i < lockTries; i++ {
		val, err := ioutil.ReadFile(lockfile)

		if err != nil {
			continue
		}

		if is_active_pid(val) {
			Debug("UNLOCKING", lockfile)
			os.RemoveAll(lockfile)
			break
		}

	}

	return true
}

func (t *Table) GrabInfoLock() bool {
	lock := Lock{Table: t, Name: "info"}
	info := &InfoLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = RecoverLock(info)
	}

	return ret
}

func (t *Table) ReleaseInfoLock() bool {
	lock := Lock{Table: t, Name: "info"}
	info := &InfoLock{lock}
	ret := info.Release()
	return ret
}

func (t *Table) GrabDigestLock() bool {
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = RecoverLock(info)
	}
	return ret
}

func (t *Table) ReleaseDigestLock() bool {
	lock := Lock{Table: t, Name: STOMACHE_DIR}
	info := &DigestLock{lock}
	ret := info.Release()
	return ret
}

func (t *Table) GrabBlockLock(name string) bool {
	lock := Lock{Table: t, Name: name}
	info := &BlockLock{lock}
	ret := info.Grab()
	// INFO RECOVER IS GOING TO HAVE TIMING ISSUES... WHEN MULTIPLE THREADS ARE
	// AT PLAY
	if !ret && info.broken {
		ret = RecoverLock(info)
	}
	return ret

}

func (t *Table) ReleaseBlockLock(name string) bool {
	lock := Lock{Table: t, Name: name}
	info := &BlockLock{lock}
	ret := info.Release()
	return ret
}

func (t *Table) GrabCacheLock() bool {
	lock := Lock{Table: t, Name: cacheDir}
	info := &CacheLock{lock}
	ret := info.Grab()
	if !ret && info.broken {
		ret = RecoverLock(info)
	}
	return ret
}

func (t *Table) ReleaseCacheLock() bool {
	lock := Lock{Table: t, Name: cacheDir}
	info := &CacheLock{lock}
	ret := info.Release()
	return ret
}
