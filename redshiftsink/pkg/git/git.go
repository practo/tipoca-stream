package git

import (
	"fmt"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/whilp/git-urls"
	"io"
	"net/url"
	"os"
	"strings"
)

type GitInterface interface {
	Clone() error
	Pull() error
	Checkout(hash string) error
	Log(fileName string, numberOfCommits int, shortHash bool) ([]string, error)
}

type Git struct {
	dir         string
	repoURL     string
	accessToken string

	// repo is initilized on Clone
	repo *git.Repository
}

func New(dir string, repoURL string, accessToken string) GitInterface {
	return &Git{
		dir:         dir,
		repoURL:     repoURL,
		accessToken: accessToken,
	}
}

func (g *Git) Clone() error {
	repo, err := git.PlainClone(g.dir, false, &git.CloneOptions{
		URL: g.repoURL,
		Auth: &http.BasicAuth{
			Username: "ts", // not used, but requires to be not empty
			Password: g.accessToken,
		},
	})

	if err != nil {
		return err
	}
	g.repo = repo

	return nil
}

func (g *Git) Pull() error {
	tree, err := g.repo.Worktree()
	if err != nil {
		return err
	}

	err = tree.Pull(&git.PullOptions{
		Auth: &http.BasicAuth{
			Username: "ts", // not used, but requires to be not empty
			Password: g.accessToken,
		},
	})

	// ignore already up to date
	if err == git.NoErrAlreadyUpToDate {
		return nil
	}

	return err
}

func (g *Git) Checkout(hash string) error {
	if g.repo == nil {
		return fmt.Errorf("repo nil cannot checkout, init repo by calling Clone()")
	}
	if hash == "" {
		return fmt.Errorf("hash version cannot be null in checkout")
	}

	tree, err := g.repo.Worktree()
	if err != nil {
		return err
	}

	return tree.Checkout(&git.CheckoutOptions{
		Hash: plumbing.NewHash(hash),
	})
}

func (g *Git) Log(
	fileName string,
	numberOfCommits int,
	shortHash bool,
) (
	[]string, error,
) {
	commitHashes := []string{}

	pathIter := func(path string) bool {
		return path == fileName
	}
	logOptions := &git.LogOptions{
		PathFilter: pathIter,
	}
	cIter, err := g.repo.Log(logOptions)
	if err != nil {
		return commitHashes, err
	}

	for i := 0; i < numberOfCommits; i++ {
		commit, err := cIter.Next()
		if err != nil {
			return commitHashes, err
		}
		hash := fmt.Sprintf("%v", commit.Hash)
		if shortHash {
			hash = hash[:7]
		}
		commitHashes = append(commitHashes, hash)
	}

	return commitHashes, nil
}

// Copy is a util function to copy file from one location to another
func Copy(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func ParseURL(pathOrURL string) (*url.URL, error) {
	return giturls.Parse(pathOrURL)
}

// ParseRepoURL breaks a path into repo and filepath
// eg: for /practo/tipoca-stream/pkg/README.md
// this: "practo/tipoca-stream" and "pkg/README.md"
func ParseGithubURL(urlPath string) (string, string) {
	filePath := strings.Join(strings.Split(urlPath, "/")[3:], "/")
	repo := strings.ReplaceAll(urlPath, "/"+filePath, "")

	return repo[1:], filePath
}
