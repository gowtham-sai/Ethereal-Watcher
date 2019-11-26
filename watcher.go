package etherealwatcher

import (
	"context"
	"fmt"

	etcd "github.com/coreos/etcd/clientv3"
)

type EtherealWatcher struct {
	*etcd.Client
}

type UpdateFunc func(string, string)

func (w *EtherealWatcher) WatchNS(ctx context.Context, ns string, f UpdateFunc) {
	etcdWatcher := etcd.NewWatcher(w.Client)
	watcherChan := etcdWatcher.Watch(ctx, ns, etcd.WithPrefix(), etcd.WithFilterDelete())

watcherLoop:
	for {
		select {
		case watcherResponse, ok := <-watcherChan:
			// channel closed, probably client asked us to stop.
			if !ok {
				fmt.Printf("EtherealWatcher closed for ns: %s\n", ns)
				break watcherLoop
			}

			if watcherResponse.Err() == nil {
				for _, event := range watcherResponse.Events {
					f(string(event.Kv.Key), string(event.Kv.Value))
				}
			} else {
				fmt.Printf("Error occurred while watching: :%+v", watcherResponse.Err())
			}
		}
	}
}

func NewWatcher(config etcd.Config) (*EtherealWatcher, error) {
	client, err := etcd.New(config)
	if err != nil {
		return nil, err
	}
	return &EtherealWatcher{client}, nil
}
