package boot

import (
	"context"
	"os"
	"time"

	storage "cloud.google.com/go/bigquery/storage/apiv1"
	"github.com/ntail-io/streams/gateway"
	"github.com/ntail-io/streams/gateway/bigquery"
	"github.com/ntail-io/streams/gateway/etcd"
	"github.com/ntail-io/streams/gateway/grpcio"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func Start(ctx context.Context) {
	vip := viper.New()
	vip.AutomaticEnv()
	vip.SetEnvPrefix("NTAIL_GW")

	topicService := gateway.NewTopicService()

	if os.Getenv("ETCD_ROOT_PASSWORD") == "" {
		_ = os.Setenv("ETCD_ROOT_PASSWORD", "pass")
	}

	bqReadClient, err := storage.NewBigQueryReadClient(ctx)
	if err != nil {
		log.WithError(err).Fatal("failed to create bigquery read client")
		return
	}

	// BigQuery
	bqReaderService := bigquery.NewReaderService(bqReadClient, vip.GetString("gcp_project_id"))

	// Etcd
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 10 * time.Second,
		Username:    vip.GetString("etcd_user"),
		Password:    vip.GetString("etcd_pass"),
	})
	if err != nil {
		log.WithError(err).Fatal("failed to create etcd client")
		return
	}

	watcher := etcd.NewWatcher(etcdClient, topicService)
	errCh := make(chan error)
	restoredCh := make(chan struct{})
	go watcher.Run(ctx, errCh, restoredCh)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				log.WithError(err).Fatal("watcher error")
				return
			}
		}
	}()
	<-restoredCh
	log.Info("watcher state restored")

	print(motdGateway)

	// GRPC
	vip.SetDefault("grpc_addr", "0.0.0.0:8081")
	addr := vip.GetString("grpc_addr")
	service := grpcio.NewGatewayService(topicService, etcdClient, bqReaderService)
	if err := grpcio.NewServer(ctx, addr, service, viper.GetBool("alts")); err != nil {
		log.WithError(err).Fatal("failed to start grpc server")
		return
	}
}

// Generated at https://manytools.org/hacker-tools/ascii-banner/
// Font ANSI Shadow
const motdGateway = ` 
███╗   ██╗████████╗ █████╗ ██╗██╗         ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗███████╗
████╗  ██║╚══██╔══╝██╔══██╗██║██║         ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║██╔════╝
██╔██╗ ██║   ██║   ███████║██║██║         ███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║███████╗
██║╚██╗██║   ██║   ██╔══██║██║██║         ╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║╚════██║
██║ ╚████║   ██║   ██║  ██║██║███████╗    ███████║   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║███████║
╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝╚══════╝    ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝
                                                                                                      
                     ██████╗  █████╗ ████████╗███████╗██╗    ██╗ █████╗ ██╗   ██╗                     
                    ██╔════╝ ██╔══██╗╚══██╔══╝██╔════╝██║    ██║██╔══██╗╚██╗ ██╔╝                     
                    ██║  ███╗███████║   ██║   █████╗  ██║ █╗ ██║███████║ ╚████╔╝                      
                    ██║   ██║██╔══██║   ██║   ██╔══╝  ██║███╗██║██╔══██║  ╚██╔╝                       
                    ╚██████╔╝██║  ██║   ██║   ███████╗╚███╔███╔╝██║  ██║   ██║                        
                     ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝ ╚══╝╚══╝ ╚═╝  ╚═╝   ╚═╝                        
                                                                                                      
`
