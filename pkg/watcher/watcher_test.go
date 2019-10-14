package watcher

import (
	"context"
	"io/ioutil"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const DefaultListenClientURL = "http://localhost:12379"

var (
	DefaultListenPeerURLs, _   = url.Parse("http://localhost:12380")
	DefaultListenClientURLs, _ = url.Parse(DefaultListenClientURL)
)

type WatcherTestSuite struct {
	wg         *sync.WaitGroup
	cfg        *embed.Config
	etcdServer *embed.Etcd

	watcher *Watcher
	suite.Suite
}

func (s *WatcherTestSuite) SetupTest() {
	etcdServer, err := embed.StartEtcd(s.cfg)
	require.NoError(s.T(), err)

	s.etcdServer = etcdServer
	s.wg = &sync.WaitGroup{}

	s.watcher, err = NewWatcher(etcd.Config{Endpoints: []string{DefaultListenClientURL}})
	require.NoError(s.T(), err)
}

func (s *WatcherTestSuite) TearDownTest() {
	s.etcdServer.Close()
}

func TestWatcherTestSuite(t *testing.T) {
	suite.Run(t, new(WatcherTestSuite))
}

func (s *WatcherTestSuite) SetupSuite() {
	tempDir, err := ioutil.TempDir(os.TempDir(), "ethereal-Watcher-test")
	require.NoError(s.T(), err)

	cfg := embed.NewConfig()
	cfg.Dir = tempDir

	cfg.LPUrls = []url.URL{*DefaultListenPeerURLs}
	cfg.LCUrls = []url.URL{*DefaultListenClientURLs}
	cfg.APUrls = []url.URL{*DefaultListenPeerURLs}
	cfg.ACUrls = []url.URL{*DefaultListenClientURLs}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	s.cfg = cfg
}

func (s *WatcherTestSuite) TearDownSuite() {
	s.etcdServer.Close()
	os.RemoveAll(s.cfg.Dir)
}

func (s *WatcherTestSuite) TestWatcher() {
	s.T().Run("Watcher should receive put events", func(t *testing.T) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		type Event struct {
			key, value string
		}
		expectedEvent := Event{"/org/domain/common", `{"boolean_flag": true}`}
		chanEvent := make(chan Event)

		go s.watcher.WatchNS(ctx, "/org/domain", func(key string, value string) {
			chanEvent <- Event{key, value}
		})

		_, err := s.watcher.Client.Put(ctx, expectedEvent.key, expectedEvent.value)
		require.NoError(t, err, "error should not occur while performing put")

		assert.Equal(t, expectedEvent, <-chanEvent)
	})

	s.T().Run("Watcher should not receive del events", func(t *testing.T) {
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()

		isCalled := false
		go s.watcher.WatchNS(ctx, "/org/domain", func(key string, value string) {
			isCalled = !isCalled
		})

		_, err := s.watcher.Client.Put(ctx, "/org/domain/common", `{"boolean_flag": true}`)
		require.NoError(t, err, "error should not occur while performing put")
		time.Sleep(time.Millisecond)
		assert.True(t, isCalled)

		_, err = s.watcher.Client.Delete(ctx, "/org/domain/common")
		require.NoError(t, err, "error should not occur while performing put")
		time.Sleep(time.Millisecond)
		assert.True(t, isCalled)
	})

	s.T().Run("context should cancel Watcher", func(t *testing.T) {
		ctx, cancelFunc := context.WithCancel(context.Background())

		isCalled := false
		go s.watcher.WatchNS(ctx, "/org/domain", func(key string, value string) {
			isCalled = !isCalled
		})

		_, err := s.watcher.Client.Put(ctx, "/org/domain/common", `{"boolean_flag": true}`)
		require.NoError(t, err, "error should not occur while performing put")
		time.Sleep(time.Millisecond)
		require.True(t, isCalled)

		cancelFunc()
		_, err = s.watcher.Client.Put(context.Background(), "/org/domain/common", `{"boolean_flag": true}`)
		require.NoError(t, err, "error should not occur while performing put")
		time.Sleep(time.Millisecond)
		assert.True(t, isCalled, "should not receive further Watcher updates")
	})
}
