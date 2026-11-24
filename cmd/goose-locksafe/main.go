// Command goose-locksafe applies goose SQL migrations to PostgreSQL with
// lock_timeout, lock-timeout retries and invalid index cleanup.
//
//	goose-locksafe [flags] up|up-by-one|up-to VERSION|down|status|version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	locksafe "github.com/nordew/goose-locksafe"
	"github.com/pressly/goose/v3"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "goose-locksafe:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("goose-locksafe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		dsn        = fs.String("dsn", os.Getenv("GOOSE_DBSTRING"), "Postgres connection string (env GOOSE_DBSTRING)")
		dir        = fs.String("dir", envOr("GOOSE_MIGRATION_DIR", "."), "migrations directory (env GOOSE_MIGRATION_DIR)")
		table      = fs.String("table", locksafe.DefaultTableName, "goose version table")
		lockTO     = fs.Duration("lock-timeout", locksafe.DefaultLockTimeout, "lock_timeout for every migration connection")
		attempts   = fs.Int("attempts", locksafe.DefaultMaxAttempts, "max attempts per migration on lock timeout")
		boBase     = fs.Duration("backoff-base", locksafe.DefaultBackoffBase, "wait after the first lock timeout, doubles per attempt")
		boMax      = fs.Duration("backoff-max", locksafe.DefaultBackoffMax, "max wait between attempts")
		outOfOrder = fs.Bool("allow-out-of-order", false, "apply missing (out-of-order) migrations")
		verbose    = fs.Bool("v", false, "log goose statements and debug output")
	)
	fs.Usage = func() {
		// Nothing useful to do if stderr is gone.
		_, _ = fmt.Fprintln(stderr, "usage: goose-locksafe [flags] up|up-by-one|up-to VERSION|down|status|version")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		fs.Usage()
		return errors.New("missing command")
	}
	if *dsn == "" {
		return errors.New("missing -dsn or GOOSE_DBSTRING")
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r, err := locksafe.New(ctx, *dsn, os.DirFS(*dir), locksafe.Config{
		LockTimeout: *lockTO,
		MaxAttempts: *attempts,
		BackoffBase: *boBase,
		BackoffMax:  *boMax,
		TableName:   *table,
		Logger:      logger,
		GooseOptions: []goose.ProviderOption{
			goose.WithAllowOutofOrder(*outOfOrder),
			goose.WithVerbose(*verbose),
			goose.WithSlog(logger),
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		if cerr := r.Close(); cerr != nil {
			logger.Warn("close", "error", cerr)
		}
	}()

	switch cmd := fs.Arg(0); cmd {
	case "up":
		res, err := r.Up(ctx)
		return errors.Join(err, printResults(stdout, res...))
	case "up-by-one":
		res, err := r.UpByOne(ctx)
		return errors.Join(err, printResults(stdout, res))
	case "up-to":
		if fs.NArg() < 2 {
			return errors.New("up-to needs a VERSION")
		}
		v, err := strconv.ParseInt(fs.Arg(1), 10, 64)
		if err != nil {
			return fmt.Errorf("bad version %q: %w", fs.Arg(1), err)
		}
		res, err := r.UpTo(ctx, v)
		return errors.Join(err, printResults(stdout, res...))
	case "down":
		res, err := r.Down(ctx)
		return errors.Join(err, printResults(stdout, res))
	case "status":
		st, err := r.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range st {
			applied := "pending"
			if s.State == goose.StateApplied {
				applied = s.AppliedAt.Format("2006-01-02 15:04:05")
			}
			if _, err := fmt.Fprintf(stdout, "%-20s %s\n", applied, s.Source.Path); err != nil {
				return err
			}
		}
		return nil
	case "version":
		v, err := r.Version(ctx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(stdout, v)
		return err
	default:
		fs.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func printResults(w io.Writer, res ...*goose.MigrationResult) error {
	for _, r := range res {
		if r == nil {
			continue
		}
		if _, err := fmt.Fprintln(w, r.String()); err != nil {
			return err
		}
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
