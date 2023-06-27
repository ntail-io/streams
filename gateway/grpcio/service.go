package grpcio

import (
	"context"

	v1 "github.com/ntail-io/streams/proto/v1"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway"
	"github.com/ntail-io/streams/gateway/bigquery"
	"github.com/ntail-io/streams/gateway/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GatewayService struct {
	topicService    *gateway.TopicService
	etcdClient      *clientv3.Client
	bqReaderService *bigquery.ReaderService
	v1.UnimplementedGatewayServiceServer
}

func NewGatewayService(
	topicService *gateway.TopicService,
	etcdClient *clientv3.Client,
	bqReaderService *bigquery.ReaderService,
) *GatewayService {
	return &GatewayService{
		topicService:    topicService,
		etcdClient:      etcdClient,
		bqReaderService: bqReaderService,
	}
}

func (s *GatewayService) CreateTopic(ctx context.Context, req *v1.CreateTopicRequest) (*empty.Empty, error) {
	topic := types.Topic(req.GetTopic())
	if err := topic.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid topic name: %s", err)
	}

	err := etcd.CreateTopic(ctx, s.etcdClient, topic, 15)
	if err != nil {
		switch err {
		case etcd.ErrAlreadyExist:
			return nil, status.Errorf(codes.AlreadyExists, "topic %s already exists", topic)
		default:
			return nil, status.Errorf(codes.Internal, "failed to create topic %s: %s", topic, err)
		}
	}

	return &empty.Empty{}, nil
}

func (s *GatewayService) ListTopics(_ context.Context, _ *v1.ListTopicsRequest) (*v1.ListTopicsResponse, error) {
	topics := s.topicService.List()

	res := &v1.ListTopicsResponse{
		Topics: make([]string, len(topics)),
	}

	for i, topic := range topics {
		res.Topics[i] = string(topic.Name)
	}

	return res, nil
}

func (s *GatewayService) CreateSubscription(ctx context.Context, req *v1.CreateSubscriptionRequest) (*empty.Empty, error) {
	topic := types.Topic(req.GetTopic())
	if err := topic.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid topic name: %s", err)
	}
	subId := types.SubscriptionId(req.GetSubscription())
	if err := subId.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid subscription id: %s", err)
	}

	if err := etcd.CreateSubscription(ctx, s.etcdClient, topic, subId); err != nil {
		switch err {
		case etcd.ErrAlreadyExist:
			return nil, status.Errorf(codes.AlreadyExists, "subscription %s already exists", subId)
		case etcd.ErrNotFound:
			return nil, status.Errorf(codes.NotFound, "topic %s not found", topic)
		default:
			return nil, status.Errorf(codes.Internal, "failed to create subscription %s: %s", subId, err)
		}
	}

	return &empty.Empty{}, nil
}

func (s *GatewayService) ListSubscriptions(_ context.Context, req *v1.ListSubscriptionsRequest) (res *v1.ListSubscriptionsResponse, err error) {
	topicName := types.Topic(req.GetTopic())
	topic, err := s.topicService.Get(topicName)
	if err != nil {
		return
	}

	subs := topic.Subscriptions()

	res = &v1.ListSubscriptionsResponse{
		Subscriptions: make([]string, len(subs)),
	}

	for i, sub := range subs {
		res.Subscriptions[i] = string(sub)
	}

	return
}
