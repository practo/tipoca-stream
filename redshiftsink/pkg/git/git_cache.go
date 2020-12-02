package git

import (
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type GitCacheInterface interface {
	GetFileVersion(filePath string) (string, error)
}

type GitCache struct {
	client           GitInterface
	cacheValidity    time.Duration
	lastCacheRefresh *int64

	cloneDir string

	// mutex protects the following the mutable state
	mutex sync.Mutex

	// fileVersion keeps a map of file name and its version
	fileVersion map[string]string
}

func NewGitCache(repoURL string, accessToken string) (GitCacheInterface, error) {
	cloneDir, err := ioutil.TempDir("", "gitcache")
	if err != nil {
		return nil, err
	}

	return &GitCache{
		client:           New(cloneDir, repoURL, accessToken),
		cacheValidity:    time.Second * time.Duration(30),
		lastCacheRefresh: nil,
		cloneDir:         cloneDir,
		fileVersion:      make(map[string]string),
	}, nil
}

// GetVersion returns the latest version of the repo file
// it fetches the latest version every cacheValidity seconds and keeps a cache
func (g *GitCache) GetFileVersion(filePath string) (string, error) {
	if cacheValid(g.cacheValidity, g.lastCacheRefresh) {
		version, ok := g.fileVersion[filePath]
		if ok {
			return version, nil
		}
	}

	g.mutex.Lock()
	defer g.mutex.Unlock()

	// clone or pull
	_, err := os.Stat(g.cloneDir)
	if os.IsNotExist(err) {
		err = g.client.Clone()
		if err != nil {
			return "", err
		}
	} else {
		err = g.client.Pull()
		if err != nil {
			return "", err
		}
	}

	// get the latest commit, update cache, return
	commits, err := g.client.Log(filePath, 1)
	if err != nil {
		return "", err
	}

	// burst cache for all files
	// recreate the fileVersion so that fileVersion for all files
	// in this repo update it again as the new pull/clone has occured
	newFileVersion := make(map[string]string)
	newFileVersion[filePath] = commits[0]
	g.fileVersion = newFileVersion
	now := time.Now().UnixNano()
	g.lastCacheRefresh = &now

	return commits[0], nil
}

func cacheValid(validity time.Duration, lastCachedTime *int64) bool {
	if lastCachedTime == nil {
		return false
	}

	if (*lastCachedTime + validity.Nanoseconds()) > time.Now().UnixNano() {
		return true
	}

	return false
}
