package boot

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ntail-io/streams/buffer"
	"github.com/ntail-io/streams/buffer/bq"
	"github.com/ntail-io/streams/buffer/buftypes"
	"github.com/ntail-io/streams/buffer/cmdhandler"
	"github.com/ntail-io/streams/buffer/domain"
	"github.com/ntail-io/streams/buffer/etcd"
	"github.com/ntail-io/streams/buffer/etcd/repo"
	"github.com/ntail-io/streams/buffer/grpcio"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func Start(ctx context.Context) {
	vip := viper.New()
	vip.AutomaticEnv()
	vip.SetEnvPrefix("NTAIL_BUFFER")

	// BigQuery
	var bqConn *bq.BigQueryConn
	{
		projectId := vip.GetString("gcp_project_id")
		// BigQuery
		managedWriter, err := managedwriter.NewClient(ctx, projectId)
		if err != nil {
			log.WithError(err).Fatal("failed to create managed writer")
			return
		}
		queryClient, err := bigquery.NewClient(ctx, projectId)
		if err != nil {
			log.WithError(err).Fatal("failed to create query client")
		}
		bqConn = &bq.BigQueryConn{
			ProjectId:    projectId,
			WriterClient: managedWriter,
			QueryClient:  queryClient,
		}
	}

	// Allocator
	allocator := domain.NewChunkAllocator(domain.WithAllocatorSize((1<<30)*4), domain.WithFullChThreshold(10))

	// Etcd
	var (
		sess *concurrency.Session
	)
	{
		etcdEndpoints := vip.GetStringSlice("etcd_endpoints")
		etcdUser := vip.GetString("etcd_user")
		etcdPass := vip.GetString("etcd_pass")

		//etcdEndpoints := []string{"localhost:2379"}
		etcdClient, err := clientv3.New(clientv3.Config{
			Endpoints:   etcdEndpoints,
			DialTimeout: 20 * time.Second,
			Username:    etcdUser,
			Password:    etcdPass,
		})
		if err != nil {
			log.WithError(err).Fatal("failed to create etcd client")
			return
		}
		sess, err = concurrency.NewSession(etcdClient, concurrency.WithTTL(10), concurrency.WithContext(ctx))
		if err != nil {
			log.WithError(err).Fatal("failed to create etcd session")
		}
		go func() {
			<-sess.Done()
			log.Fatal("etcd session closed")
		}()
	}

	// Address
	var addr string
	{
		hostname, err := os.Hostname()
		if err != nil {
			log.WithError(err).Fatal("failed to get hostname")
			return
		}
		vip.SetDefault("grpc_addr", hostname)
		vip.SetDefault("grpc_port", 8080)
		addr = fmt.Sprintf("%s:%d", vip.GetString("grpc_addr"), vip.GetInt("grpc_port"))
	}

	segmentRepo := repo.NewSegmentRepo(sess, addr)

	vip.SetDefault("max_segments", 5_000)

	finishedSegmentsCh := make(chan *buftypes.SegmentAddress, vip.GetInt("max_segments"))

	router := buffer.NewRouter(ctx, segmentRepo, allocator, finishedSegmentsCh)

	vip.SetDefault("bq_flush", true)

	segProps := cmdhandler.SegmentServiceProps{
		Router:       router,
		Repo:         segmentRepo,
		Conn:         bqConn,
		FlushEnabled: vip.GetBool("bq_flush"),
	}

	_ = etcd.NewLeaseService(ctx, addr, sess, segProps)

	go buffer.RunSegCloser(ctx, router, allocator, finishedSegmentsCh)

	if err := grpcio.NewServer(ctx, addr, grpcio.NewBufferService(router), viper.GetBool("alts")); err != nil {
		log.WithError(err).Fatal("failed to create grpc server")
		return
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(":8090", nil)
		if err != nil {
			log.WithError(err).Fatal("failed to start prometheus server")
		}
	}()

	log.Infof("buffer started!")
	print(bufferMotd)
}

const bufferMotd = `
███╗   ██╗████████╗ █████╗ ██╗██╗         ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗███████╗
████╗  ██║╚══██╔══╝██╔══██╗██║██║         ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║██╔════╝
██╔██╗ ██║   ██║   ███████║██║██║         ███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║███████╗
██║╚██╗██║   ██║   ██╔══██║██║██║         ╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║╚════██║
██║ ╚████║   ██║   ██║  ██║██║███████╗    ███████║   ██║   ██║  ██║███████╗██║  ██║██║a ╚═╝ ██║███████║
╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚═╝╚══════╝    ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝
                                                                                                      
                            ██████╗ ██╗   ██╗███████╗███████╗███████╗██████╗                          
                            ██╔══██╗██║   ██║██╔════╝██╔════╝██╔════╝██╔══██╗                         
                            ██████╔╝██║   ██║█████╗  █████╗  █████╗  ██████╔╝                         
                            ██╔══██╗██║   ██║██╔══╝  ██╔══╝  ██╔══╝  ██╔══██╗                         
                            ██████╔╝╚██████╔╝██║     ██║     ███████╗██║  ██║                         
                            ╚═════╝  ╚═════╝ ╚═╝     ╚═╝     ╚══════╝╚═╝  ╚═╝                         
                                                                                                      
`
