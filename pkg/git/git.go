package git

import (
	"fmt"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"io"
	"os"
)

type GitInterface interface {
	Clone() error
	Checkout(hash string) error
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

func (g *Git) Checkout(hash string) error {
	if g.repo == nil {
		return fmt.Errorf("repo nil cannot checkout, init repo by calling Clone()")
	}

	tree, err := g.repo.Worktree()
	if err != nil {
		return err
	}

	var checkoutOptions *git.CheckoutOptions
	if hash != "" {
		checkoutOptions = &git.CheckoutOptions{
			Hash: plumbing.NewHash(hash),
		}
	} else {
		checkoutOptions = &git.CheckoutOptions{}
	}
	err = tree.Checkout(checkoutOptions)

	return err
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
