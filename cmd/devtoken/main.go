package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/rowjay007/collab-sphere/pkg/auth"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Getenv("AUTH_PRIVATE_KEY")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out interface{ Write([]byte) (int, error) }, privEnv string) error {
	fs := flag.NewFlagSet("devtoken", flag.ContinueOnError)
	fs.SetOutput(new(discard))
	gen := fs.Bool("gen", false, "print a fresh keypair")
	subject := fs.String("subject", "", "user handle")
	ttl := fs.Duration("ttl", 15*time.Minute, "token lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *gen {
		pub, priv, err := auth.GenerateKeypair()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "AUTH_PUBLIC_KEY=%s\nAUTH_PRIVATE_KEY=%s\n", auth.EncodeKey(pub), auth.EncodeKey(priv.Seed()))
		return err
	}
	if *subject == "" {
		return fmt.Errorf("subject is required")
	}
	if privEnv == "" {
		return fmt.Errorf("AUTH_PRIVATE_KEY is required")
	}
	priv, err := auth.ParsePrivateKey(privEnv)
	if err != nil {
		return err
	}
	now := time.Now()
	tok, err := auth.Mint(priv, *subject, now, now.Add(*ttl))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, tok)
	return err
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
