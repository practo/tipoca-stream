package git

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type GitCacheInterface interface {
	GetFileVersion(filePath string) (string, error)
	GetFileLocalPath(filepath string) string
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
	now := time.Now().UnixNano()

	g.mutex.Lock()
	defer g.mutex.Unlock()

	// clone or pull
	_, err := os.Stat(filepath.Join(g.cloneDir, ".git"))
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

	// new cache
	newFileVersion := make(map[string]string)

	// update new cache for filePath
	commits, err := g.client.Log(filePath, 1)
	if err != nil {
		return "", err
	}
	newFileVersion[filePath] = commits[0]

	// update new cache for all the other files that already existed
	for path, _ := range g.fileVersion {
		// get the latest commit, update cache
		commits, err := g.client.Log(filePath, 1)
		if err != nil {
			// this could happen if the filePath does not exist, so burst
			// the cache so that next update fixes it.
			g.fileVersion = make(map[string]string)
			g.lastCacheRefresh = &now
			return "", err
		}
		newFileVersion[path] = commits[0]
	}

	// replace the cache with the new cache
	g.fileVersion = newFileVersion

	// update last cache update time
	g.lastCacheRefresh = &now

	return commits[0], nil
}

// GetFileLocalPath takes the relative path of the file from repo
// and returns the local path the file should be present if downloaded
func (g *GitCache) GetFileLocalPath(filePath string) string {
	return filepath.Join(g.cloneDir, filePath)
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
