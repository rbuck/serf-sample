package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"continuul.io/test/lib"
	uuid "github.com/hashicorp/go-uuid"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/spf13/cobra"
)

var (
	serfLANSnapshot = "serf/local.snapshot"
)

const (
	rpcPort                    = 9310
	serverPort                 = 9311
	defaultRaftMultiplier uint = 5
)

type Config struct {
	nodeId           string
	nodeName         string
	bindAddress      string
	bindPort         int
	advertiseAddress string
	advertisePort    int
	joinAddresses    lib.ListOpts
	dataDirectory    string
}

type Server struct {
	config       *Config
	logger       *log.Logger
	serfConfig   *serf.Config
	serfLan      *serf.Serf
	eventCh      chan serf.Event
	leaveCh      chan struct{}
	reconcileCh  chan serf.Member
	isShutdown   bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func setupServerConfig(c *Config) error {
	// defaults...
	if c.nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("Error determining hostname: %s", err)
		}
		c.nodeName = hostname
	}

	err := setupNodeId(c)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) defaultSerfConfig() *serf.Config {
	conf := serf.DefaultConfig()
	conf.CoalescePeriod = 3 * time.Second
	conf.QuiescentPeriod = time.Second
	conf.UserCoalescePeriod = 3 * time.Second
	conf.UserQuiescentPeriod = time.Second
	conf.ReconnectTimeout = 3 * 24 * time.Hour
	conf.MemberlistConfig = memberlist.DefaultLANConfig()
	conf.MemberlistConfig.SuspicionMult = 2
	conf.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	conf.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	conf.MemberlistConfig.GossipInterval = 100 * time.Millisecond
	return conf
}

func (s *Server) defaultRaftConfig() *serf.Config {
	// conf.ScaleRaft(defaultRaftMultiplier)
	// // Disable shutdown on removal
	// conf.RaftConfig.ShutdownOnRemove = false
	// // Check every 5 seconds to see if there are enough new entries for a snapshot
	// conf.RaftConfig.SnapshotInterval = 5 * time.Second
	return nil
}

func (s *Server) configureSerf(c *Config, path string) error {
	conf := s.defaultSerfConfig()

	if c.nodeId != "" {
		conf.NodeName = c.nodeId
	}

	conf.MemberlistConfig.BindAddr = c.bindAddress
	conf.MemberlistConfig.BindPort = c.bindPort

	if c.advertiseAddress != "" {
		conf.MemberlistConfig.AdvertiseAddr = c.advertiseAddress
	} else if c.bindAddress != "0.0.0.0" && c.bindAddress != "" && c.bindAddress != "[::]" {
		conf.MemberlistConfig.AdvertiseAddr = c.bindAddress
	} else {
		var err error
		var ip net.IP
		if c.bindAddress == "[::]" {
			ip, err = lib.GetPublicIPv6()
		} else {
			ip, err = lib.GetPrivateIP()
		}
		if err != nil {
			return fmt.Errorf("Failed to get advertise address: %v", err)
		}
		conf.MemberlistConfig.AdvertiseAddr = ip.String()
	}
	conf.MemberlistConfig.AdvertisePort = c.advertisePort

	conf.SnapshotPath = filepath.Join(s.config.dataDirectory, path)
	if err := lib.EnsurePath(conf.SnapshotPath, false); err != nil {
		return err
	}

	s.serfConfig = displayConfig(conf)

	return nil
}

func displayConfig(config *serf.Config) *serf.Config {
	fmt.Println("Configuration:")
	fmt.Println(fmt.Sprintf("            node id: %s", config.NodeName))
	fmt.Println(fmt.Sprintf("       bind address: %s", config.MemberlistConfig.BindAddr))
	fmt.Println(fmt.Sprintf("          bind port: %d", config.MemberlistConfig.BindPort))
	fmt.Println(fmt.Sprintf("  advertise address: %s", config.MemberlistConfig.AdvertiseAddr))
	fmt.Println(fmt.Sprintf("     advertise port: %d", config.MemberlistConfig.AdvertisePort))
	return config
}

var gracefulTimeout = 5 * time.Second

// handleSignals blocks until we get an exit-causing signal
func (s *Server) handleSignals() int {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for a signal
	for {
		var sig os.Signal
		select {
		case s := <-signalCh:
			sig = s
		case <-s.shutdownCh:
			// agent is already shut down
			return 0
		}
		s.logger.Println(fmt.Sprintf("[INFO] Caught signal: %v", sig))

		switch sig {
		// Skip SIGPIPE signals
		case syscall.SIGHUP:
			// reload config...
			continue
		case syscall.SIGPIPE:
			continue
		default:
			// Check if we should do a graceful leave
			graceful := true
			if sig == os.Interrupt {
				graceful = true
			} else if sig == syscall.SIGTERM {
				graceful = true
			}

			// Bail fast if not doing a graceful leave
			if !graceful {
				s.logger.Println("Bailing fast...")
				return 1
			}

			// Attempt a graceful leave
			gracefulCh := make(chan struct{})
			s.logger.Println("Gracefully shutting down agent...")
			go func() {
				if err := s.leave(); err != nil {
					s.logger.Println(fmt.Sprintf("Error: %s", err))
					return
				}
				close(gracefulCh)
			}()

			// Wait for leave or another signal
			select {
			case <-signalCh:
				s.logger.Println("<-signalCh")
				return 1
			case <-time.After(gracefulTimeout):
				s.logger.Println("<-time.After(gracefulTimeout)")
				return 1
			case <-gracefulCh:
				s.logger.Println("<-gracefulCh")
				return 0
			}
		}
	}
}

func (s *Server) serfEventHandler() {
	serfShutdownCh := s.serfLan.ShutdownCh()
	for {
		select {
		case e := <-s.eventCh:
			switch e.EventType() {
			case serf.EventMemberJoin:
				s.logger.Println("EventMemberJoin")
			//s.lanNodeJoin(e.(serf.MemberEvent))
			//s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				s.logger.Println("EventMemberLeave|EventMemberFailed")
			//s.lanNodeFailed(e.(serf.MemberEvent))
			//s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventMemberReap:
				s.logger.Println("EventMemberReap")
			//s.localMemberEvent(e.(serf.MemberEvent))
			case serf.EventUser:
				s.logger.Println("EventUser")
			//s.localEvent(e.(serf.UserEvent))
			case serf.EventMemberUpdate: // Ignore
				s.logger.Println("EventMemberUpdate")
			case serf.EventQuery: // Ignore
				s.logger.Println("EventQuery")
			default:
				s.logger.Println(fmt.Sprintf("[WARN] on: Unhandled LAN Serf Event: %#v", e))
			}
		case <-serfShutdownCh:
			s.logger.Println("[WARN] agent: Serf shutdown detected, quitting")
			s.shutdown()
			return
		case <-s.shutdownCh:
			s.logger.Println("[WARN] agent: Serf shutdown detected, quitting")
			return
		}
	}
}

func (s *Server) joinLan(addrs []string) (int, error) {
	count, err := s.serfLan.Join(addrs, true)
	if err != nil {
		s.logger.Println(fmt.Sprintf("Couldn't join cluster, starting own: %v", err))
	}
	return count, err
}

func validateJoin(val string) (string, error) {
	return val, nil
}

func (s *Server) setupSerf(eventCh chan serf.Event, logOutput io.Writer) (*serf.Serf, error) {
	s.serfConfig.Init()
	s.serfConfig.EventCh = eventCh
	s.serfConfig.EnableNameConflictResolution = false
	s.serfConfig.LogOutput = logOutput
	s.serfConfig.MemberlistConfig.LogOutput = logOutput
	return serf.Create(s.serfConfig)
}

func newServer(c *Config) (*Server, error) {
	err := setupServerConfig(c)
	if err != nil {
		return nil, fmt.Errorf("Error: couldn't setup server config: %s", err)
	}

	logOutput := os.Stderr

	s := &Server{
		config:      c,
		logger:      log.New(logOutput, "", log.LstdFlags),
		eventCh:     make(chan serf.Event, 256),
		leaveCh:     make(chan struct{}),
		shutdownCh:  make(chan struct{}),
		reconcileCh: make(chan serf.Member, 32),
	}

	err = s.configureSerf(c, serfLANSnapshot)
	if err != nil {
		return nil, fmt.Errorf("Error: couldn't configure serf: %s", err)
	}

	serfLan, err := s.setupSerf(s.eventCh, logOutput)
	if err != nil {
		s.shutdown()
		return nil, fmt.Errorf("Error: couldn't create cluster: %s", err)
	}
	s.serfLan = serfLan

	// start event loop
	go s.serfEventHandler()
	return s, nil
}

func startServer(c *Config) error {
	s, err := newServer(c)
	if err != nil {
		return fmt.Errorf("Error creating server: %s", err)
	}
	defer s.shutdown()

	s.joinLan(c.joinAddresses.GetAll())

	s.handleSignals()

	return nil
}

func (s *Server) leave() error {
	s.logger.Println("[INFO] server starting leave")
	if s.serfLan != nil {
		if err := s.serfLan.Leave(); err != nil {
			s.logger.Println(fmt.Sprintf("[ERROR] failed to leave Serf cluster: %v", err))
		}
	}

	return nil
}

func (s *Server) shutdown() error {
	s.logger.Println("[INFO] shutting down server")
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.isShutdown {
		return nil
	}

	if s.serfLan != nil {
		s.logger.Println("[INFO] agent: requesting serf shutdown")
		if err := s.serfLan.Shutdown(); err != nil {
			return err
		}
	}

	s.logger.Println("[INFO] agent: shutdown complete")
	s.isShutdown = true
	close(s.shutdownCh)
	return nil
}

// makeRandomID will generate a random UUID for a node.
func makeRandomId() (string, error) {
	id, err := uuid.GenerateUUID()
	if err != nil {
		return "", err
	}

	fmt.Println(fmt.Sprintf("[DEBUG] Using random ID %q as node ID", id))
	return id, nil
}

// setupNodeID will pull the persisted node ID, if any, or create a random one
// and persist it.
func setupNodeId(config *Config) error {
	// If they've configured a node ID manually then just use that, as
	// long as it's valid.
	if config.nodeId != "" {
		if _, err := uuid.ParseUUID(string(config.nodeId)); err != nil {
			return err
		}

		return nil
	}

	// Load saved state, if any. Since a user could edit this, we also
	// validate it.
	fileID := filepath.Join(config.dataDirectory, "node-id")
	if _, err := os.Stat(fileID); err == nil {
		rawID, err := ioutil.ReadFile(fileID)
		if err != nil {
			return err
		}

		nodeId := strings.TrimSpace(string(rawID))
		if _, err := uuid.ParseUUID(nodeId); err != nil {
			return err
		}

		config.nodeId = nodeId
	}

	// If we still don't have a valid node ID, make one.
	if config.nodeId == "" {
		id, err := makeRandomId()
		if err != nil {
			return err
		}
		if err := lib.EnsurePath(fileID, false); err != nil {
			return err
		}
		if err := ioutil.WriteFile(fileID, []byte(id), 0600); err != nil {
			return err
		}

		config.nodeId = id
	}
	return nil
}

func main() {
	config := &Config{
		joinAddresses: lib.NewListOpts(validateJoin),
	}
	var cmd = &cobra.Command{
		Use:   "demo",
		Short: "Demonstrates serf and raft interaction",
		Long: `A demonstration of establishing concensus and maintaining
					distributed view of state, and react to events such
					as node arrival and leaving.`,
		Run: func(cmd *cobra.Command, args []string) {
			err := startServer(config)
			if err != nil {
				fmt.Println(fmt.Sprintf("Error starting server: %s", err))
				os.Exit(1)
			}
		},
	}
	cmd.PersistentFlags().StringVar(&config.nodeName, "name", "", "node name")
	cmd.PersistentFlags().StringVarP(&config.bindAddress, "bind", "b", "0.0.0.0", "server bind address")
	cmd.PersistentFlags().IntVar(&config.bindPort, "bind-port", serverPort, "bind address port")
	cmd.PersistentFlags().StringVar(&config.advertiseAddress, "advertise-address", "", "advertise address")
	cmd.PersistentFlags().IntVar(&config.advertisePort, "advertise-port", serverPort, "advertise address port")
	cmd.PersistentFlags().Var(&config.joinAddresses, "join", "address of other node to join on startup")
	cmd.PersistentFlags().StringVar(&config.dataDirectory, "data-dir", "", "data directory")

	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
