package gateway

import (
	"errors"
	"sync"

	"github.com/ntail-io/streams/core/types"
	"github.com/ntail-io/streams/gateway/domain"
)

var (
	ErrInvalidTopicName = errors.New("invalid topic name")
)

type TopicService struct {
	topics map[types.Topic]*domain.Topic
	mu     sync.RWMutex
}

func NewTopicService() *TopicService {
	return &TopicService{
		topics: make(map[types.Topic]*domain.Topic),
	}
}

func (s *TopicService) List() ([]*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]*domain.Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}

	return topics, nil
}

func (s *TopicService) Get(name types.Topic) (*domain.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topic, ok := s.topics[name]
	if !ok {
		return nil, ErrInvalidTopicName
	}

	return topic, nil
}

func (s *TopicService) Add(name types.Topic, size types.HashRangeSize) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, ok := s.topics[name]
	if !ok {
		topic = domain.NewTopic(name, size)
		s.topics[name] = topic
	} else {
		topic.HashRangeSize(size)
	}

	return nil
}
